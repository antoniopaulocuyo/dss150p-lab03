import pandas as pd
from src.config import path_for
from src.common.audit import utc_now_iso, record_hash


HASH_KEYS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'quantity', 'unit_price', 'discount_pct', 'status',
    'gross_amount', 'discount_amount', 'net_amount',
]


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows.

    Orphan customer/product references are quarantined with a reason rather
    than silently dropped. Returns (curated_df, quarantine_df).
    """
    orders = staging['orders']
    customers = staging['customers']
    products = staging['products']

    # Merge with indicator to detect orphan foreign keys explicitly.
    merged = orders.merge(
        customers[['customer_id', 'city', 'customer_tier']],
        on='customer_id', how='left', indicator='customer_match'
    ).merge(
        products[['product_id', 'name', 'category', 'brand']],
        on='product_id', how='left', indicator='product_match'
    )

    orphan_customer = merged['customer_match'] == 'left_only'
    orphan_product = merged['product_match'] == 'left_only'
    orphan = orphan_customer | orphan_product

    quarantine_records = []
    for idx, row in merged[orphan].iterrows():
        reasons = []
        if orphan_customer.loc[idx]:
            reasons.append('orphan_customer_id')
        if orphan_product.loc[idx]:
            reasons.append('orphan_product_id')
        quarantine_records.append({
            'source_table': 'orders_curated_join',
            'business_key': row['order_id'],
            'reason': ';'.join(reasons),
            'pipeline_run_id': run_id,
            'quarantined_at_utc': utc_now_iso(),
            'payload': row.drop(labels=['customer_match', 'product_match']).to_json(),
        })

    valid = merged[~orphan].drop(columns=['customer_match', 'product_match']).copy()

    valid = valid.rename(columns={'city': 'customer_city', 'name': 'product_name'})

    valid['gross_amount'] = valid['quantity'] * valid['unit_price']
    valid['discount_amount'] = valid['gross_amount'] * valid['discount_pct']
    valid['net_amount'] = valid['gross_amount'] - valid['discount_amount']

    valid['source_updated_at'] = valid['updated_at']
    valid['pipeline_run_id'] = run_id
    valid['processed_at_utc'] = utc_now_iso()

    valid['record_hash'] = valid.apply(
        lambda r: record_hash(r.to_dict(), HASH_KEYS), axis=1
    )

    curated_cols = [
        'order_id', 'customer_id', 'product_id', 'order_timestamp',
        'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
        'quantity', 'unit_price', 'discount_pct',
        'gross_amount', 'discount_amount', 'net_amount', 'status',
        'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
    ]
    curated_df = valid[curated_cols].reset_index(drop=True)
    quarantine_df = pd.DataFrame(quarantine_records)

    curated_dir = path_for('curated_dir')
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated_df.to_parquet(curated_dir / 'sales_order_lines.parquet', index=False)

    if not quarantine_df.empty:
        quarantine_dir = path_for('quarantine_dir')
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_df.to_parquet(
            quarantine_dir / f'curated_quarantine_{run_id}.parquet', index=False
        )

    return curated_df, quarantine_df