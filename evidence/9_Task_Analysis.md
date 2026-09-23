# Goal 3 — Analysis Questions

## 1. Which file format was smallest, and what explains it?

Parquet (Snappy) came in smallest at 5,454,047 bytes, compared to 15,226,777
for CSV and 33,009,302 for JSON Lines. That's about 2.8x smaller than CSV
and 6x smaller than JSONL.

The main reason is that Parquet stores data by column instead of by row, so
similar values sit next to each other and compress much better — status
only has 6 possible values across ~50,000 rows, for example, which
compresses extremely well in a columnar layout. It also stores numbers as
actual binary numbers instead of decimal text, and only stores each column
name once instead of repeating it on every row. JSON Lines does the
opposite of all of this — every field name gets repeated on every single
line — which is why it ended up being the biggest format by far, even
bigger than plain CSV.

## 2. Which representation was fastest for a full read? Does that imply it's best for every workload?

Parquet again, at 0.013s median vs 0.097s for CSV, 0.222s for JSONL, and
0.545s for Postgres. That's over 40x faster than reading the same data out
of the database.

But that doesn't mean Parquet wins for everything — it just wins at "load
one static file into memory as fast as possible." Postgres is doing a
completely different job: multiple people can read and write to it at the
same time with actual consistency guarantees, and it supports updating a
single row without touching the rest of the file. That's exactly why
Goal 2 uses Postgres with UPSERT instead of just writing to Parquet —
Parquet files are basically read-only artifacts once written; changing one
row usually means rewriting the whole file. So "fastest full read" and
"best storage choice" are answering two different questions.

## 3. How did filtered retrieval differ between Parquet and PostgreSQL? What could change the result?

Filtering for status = 'DELIVERED', Parquet's median was 0.007s vs 0.113s
for Postgres — roughly 16x faster. Postgres's filtered query was still
much faster than its own full read (0.545s), but Parquet still won
outright.

Parquet can skip whole chunks of data it knows don't contain any
DELIVERED rows just by checking stored min/max stats per row group,
without even opening them. Postgres, on the other hand, is almost
certainly doing a sequential scan through all 49,897 rows since there's no
index on status. Running:

```sql
EXPLAIN ANALYZE SELECT * FROM curated.sales_order_lines WHERE status = 'DELIVERED';
```

would confirm this (look for "Seq Scan" vs "Index Scan" in the plan).
Adding an index:

```sql
CREATE INDEX idx_sales_order_lines_status ON curated.sales_order_lines (status);
```

would let Postgres jump straight to the matching rows instead of checking
every single one, which should close a lot of that gap — though it'll
still have some network/protocol overhead a local file read doesn't.

## 4. Why is JSON Lines more pipeline-friendly than one big JSON array?

JSONL is just one complete JSON object per line, instead of one big array
wrapping everything. That makes a real difference for pipelines:

- You can append a new record by just writing one more line. Appending to
  a JSON array means reading the whole file, editing the closing bracket,
  and rewriting it — or regenerating the entire thing.
- You can process it one line at a time without loading the whole file
  into memory. A JSON array generally isn't valid (or parseable) until
  you've seen its closing bracket, so most parsers need the whole thing
  first.
- If something crashes mid-write, every line written so far in a JSONL
  file is still valid, usable data. A half-written JSON array is usually
  just broken.

The downside — which shows up directly in this benchmark — is that JSONL
gives up the compactness of declaring a schema once. Repeating every field
name on every row is exactly why it was the biggest and slowest format
here, even though it holds the same data as everything else.

## 5. What happens if a partition key has extremely high cardinality or poor query locality?

If you partition on something with too many unique values (like order_id
or customer_id), you end up with a huge number of tiny partitions —
possibly one file per value. That backfires in a few ways: file-open and
metadata overhead starts costing more than the actual data scan, the
process of figuring out which partitions to even look at becomes a
bottleneck by itself, and compression gets worse since there's less data
per file for it to work with.

Poor locality is a separate problem — it's when the partition key doesn't
match how people actually query the data. If everyone filters by
customer_city but the data's partitioned by year/month, every query still
has to scan every partition anyway, since the partitioning gives zero
benefit for that filter. You've added complexity without getting any of
the I/O savings partitioning is supposed to provide.

The order_year/order_month setup used here avoids both problems — it
produces a reasonable number of partitions (21, roughly 2,000–2,500 rows
each) and lines up with a realistic query pattern (pulling a specific
month of orders), which is exactly the case partition pruning is meant
to help with.