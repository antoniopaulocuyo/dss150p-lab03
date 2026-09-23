`build_curated()` joins staged orders to staged customers and products,
computes `gross_amount = quantity * unit_price`,
`discount_amount = gross_amount * discount_pct`,
`net_amount = gross_amount - discount_amount`, and adds
`source_updated_at`, `pipeline_run_id`, `processed_at_utc`, and a
deterministic `record_hash` (excludes run-varying fields so identical
business content hashes identically across reruns).

`unit_price` for money calculations is taken from the **order** record
(transaction-time price), not the product catalog, since these can
legitimately diverge over time and the order's price is what was actually
charged.

### Execution Results

- **Curated rows:** 49,897
- **Curated quarantined:** 101

### Orphan Reference Investigation

The curated quarantine count (101) was significantly higher than the
1 orphan product reference identified during raw-data profiling. Investigation
found:

- **99 orders** reference `product_id = P0078` — the same product that was
  quarantined at the *staging* layer for having a negative price. Once
  removed from staged products, every order referencing it lost its join
  match and was flagged `orphan_product_id` at the curated step.
- **1 order** references a `product_id` that never existed in the raw source
  at all — a true orphan.
- **1 order** references a `customer_id` that never existed in the raw source
  — a true orphan.
- Total: 99 + 1 + 1 = 101, matching the observed quarantine count.

**Decision:** the current implementation labels all 101 rows as
`orphan_customer_id` / `orphan_product_id`, which is technically correct
(the referenced row is absent from the *cleaned* staging dataset) but
imprecise — 99 of the 101 are a downstream consequence of a single upstream
data-quality rejection (`P0078`), not 99 independent referential-integrity
failures. Given submission time constraints, this distinction is documented
here rather than implemented as a separate quarantine reason code. A future
improvement would pass the staging quarantine business-key set into
`build_curated` so it can distinguish `product_quarantined_upstream` from
`product_never_existed`, giving more precise root-cause visibility without
changing the row counts or curated output.