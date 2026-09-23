# Technical Questions

## 1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?

It gives you a cheap way to tell "did this row's actual content change" apart
from "did we just run the pipeline again." Without it, every UPSERT would
rewrite every row every time, even when nothing about the order actually
changed — wasted I/O at best, and it makes it harder to tell a real update
from a no-op rerun in something like `pg_stat_user_tables`.

Columns that shouldn't go into it: `pipeline_run_id` and `processed_at_utc`
(and any other run-specific timestamp). Those change on literally every
run by definition, so if they were part of the hash, the hash would never
match twice and the guard would be useless — you'd be back to rewriting
every row every time. We actually ran into a live example of why this
exclusion matters during Goal 4: after excluding them from the hash, an
identical rerun correctly skipped rewriting business columns, but that
also meant `pipeline_run_id` itself didn't advance, which we had to fix
with a separate unconditional touch-update for just those two columns.

## 2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?

Because staging and curated logic will be wrong or need to change at some
point, and when that happens you need to be able to go back and reprocess
from the actual source, not from an already-cleaned version that might be
missing the exact problem you're trying to fix. If a bug in `build_staging`
silently drops rows for six months before anyone notices, having the raw
snapshots means you can reprocess history correctly. Without them, that
history is just gone — the original defect record (like which product had
the negative price, or which order had status UNKNOWN) only exists in the
raw copy, since staging quarantines and removes it.

## 3. What is the difference between a data-quality rejection and a system exception?

A data-quality rejection means the pipeline is working correctly and the
*data* is the problem — a negative price, an out-of-range quantity, an
orphan foreign key. These get quarantined with a reason, not thrown, and
the pipeline keeps running past them.

A system exception means the pipeline itself can't proceed — a missing
source file, a broken database connection, a stage crashing partway
through. These get raised (we wrap them in `PipelineStageError` with the
stage name and run_id) and stop that run, because there's nothing sensible
to quarantine — the run just failed.

The practical test we used: if the row is present and readable but its
values violate a business rule, it's quarantine. If the pipeline can't
even get to the point of evaluating the rule, it's an exception.

## 4. Why might Parquet outperform CSV for the same rows?

Because CSV forces you to parse every field as text on every read — numbers,
dates, everything comes back as strings that then need to be cast. Parquet
stores typed, columnar binary data, so reading it means loading already-typed
arrays directly, no parsing step. This showed up directly in our benchmark:
Parquet's full read was about 7x faster than CSV's (0.013s vs 0.097s) on the
exact same 49,897 rows. Columnar layout also means a filtered query only has
to touch the one column being filtered on, plus it can skip whole row groups
using stored min/max stats — CSV has no such shortcut, it has to read the
whole file top to bottom regardless of what you're filtering for.

## 5. Why is a DAG with all the transformation logic directly in it harder to maintain?

A few reasons that all point the same direction: you can't test the logic
outside of Airflow (no unit tests against `build_curated()` directly, you'd
have to actually run a DAG to check anything), you can't reuse it (if you
wanted to run the same transform from a script or a different scheduler,
you'd have to copy-paste the DAG's code), and changes to business logic
force changes to orchestration code that has nothing to do with the change
you're making. It also blurs the line between "what does this pipeline do"
and "when/how does it run," which are genuinely different concerns. Our DAG
just calls `python -m src.cli extract/transform/load/validate` — the actual
logic lives in `src/`, gets tested independently of Airflow, and the DAG's
only job is sequencing, retries, and parameters.

## 6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.

Retries assume that running a task again after a failure is safe — that
re-running it doesn't make things worse than not retrying at all. That's
only true if the task is idempotent.

Concrete example from our own pipeline: if `upsert_curated` used plain
`INSERT` instead of `ON CONFLICT ... DO UPDATE`, then a task that partially
succeeds — say it inserts 30,000 of 49,897 rows, then the connection drops —
would get retried by Airflow, and the retry would try to insert those same
30,000 rows again, hitting a primary key violation, or worse, if `order_id`
weren't a primary key at all, silently creating 30,000 duplicate rows. We
avoided this entirely by keying the UPSERT on `order_id` — a retry (or a
Goal 4 partition reload, which we tested directly) just re-applies the same
end state instead of compounding a partial write.

## 7. What trade-off does partitioning too aggressively introduce?

You trade query-time savings for overhead somewhere else. Too many, too-small
partitions means more files, more metadata to read just to figure out which
partitions to even look at, and worse compression per file since there's
less redundant data in each chunk for something like Snappy to work with.
At some point the cost of managing all those tiny partitions outweighs
whatever I/O you saved by pruning — you can end up slower than just reading
one unpartitioned file. Our `order_year`/`order_month` scheme produced 21
partitions of roughly 2,000–2,500 rows each, which is a reasonable middle
ground — partitioning by something like `order_id` instead would have
produced tens of thousands of one-row partitions, which would be actively
worse than not partitioning at all.

## 8. How would you adapt the pipeline if the source became an API or database instead of local files?

The place to change is `src/extract/` only — that's the whole point of
keeping extraction as its own module with a narrow responsibility.
`extract_sources()` would swap from `shutil.copy2` calls to API requests or
a database query, but it would still write the results into the same
run-specific `data/raw/run_id=.../` structure, still return that raw
directory, and still raise on failure instead of silently returning
partial data. Nothing downstream — staging, curated, load, validate,
benchmark — would need to change at all, since they only ever consume
whatever lands in `data/raw/`, they don't know or care whether it
originally came from a CSV file or an API. The one thing that would
become newly important is incremental extraction (e.g. "only fetch orders
updated since the last successful run") to avoid re-pulling the entire
dataset from an external API every single run — but that's an extension of
the same `extract_sources(run_id)` contract, not a redesign of it.