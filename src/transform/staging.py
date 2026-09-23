import json
import pandas as pd
from src.config import path_for, SETTINGS
from src.common.audit import utc_now_iso


def _quarantine_record(source_table, business_key, reason, payload, run_id):
    return {
        'source_table': source_table,
        'business_key': business_key,
        'reason': reason,
        'pipeline_run_id': run_id,
        'quarantined_at_utc': utc_now_iso(),
        'payload': json.dumps(payload, default=str),
    }


def _dedupe_latest(df: pd.DataFrame, key: str, updated_col: str = 'updated_at') -> pd.DataFrame:
    """Keep only the most recent row per business key."""
    return (
        df.sort_values(updated_col, ascending=False)
          .drop_duplicates(subset=key, keep='first')
          .reset_index(drop=True)
    )


def _stage_customers(raw_dir, run_id: str) -> pd.DataFrame:
    df = pd.read_csv(raw_dir / 'customers.csv')
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)
    df = _dedupe_latest(df, 'customer_id')

    df['email'] = df['email'].str.strip().str.lower()  # NaN stays NaN, visible downstream
    df['city'] = df['city'].str.strip().str.title()

    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = utc_now_iso()
    return df


def _stage_products(raw_dir, run_id: str, quarantine: list) -> pd.DataFrame:
    with (raw_dir / 'products.json').open(encoding='utf-8') as f:
        raw = json.load(f)
    df = pd.json_normalize(raw)
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)
    df = _dedupe_latest(df, 'product_id')

    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    invalid_price = df['unit_price'].isna() | (df['unit_price'] <= 0)

    for _, row in df[invalid_price].iterrows():
        quarantine.append(_quarantine_record(
            'products', row['product_id'], 'invalid_or_nonpositive_price',
            row.to_dict(), run_id
        ))

    df = df[~invalid_price].copy()
    df = df.rename(columns={'category.name': 'category', 'category.department': 'department'})

    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = utc_now_iso()
    return df


def _stage_orders(raw_dir, run_id: str, quarantine: list) -> pd.DataFrame:
    df = pd.read_csv(raw_dir / 'orders.csv')
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], utc=True)
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True)
    df = _dedupe_latest(df, 'order_id')

    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    min_q = SETTINGS['quality']['min_quantity']
    max_q = SETTINGS['quality']['max_quantity']
    allowed_statuses = set(SETTINGS['quality']['allowed_order_statuses'])

    bad_quantity = df['quantity'].isna() | (df['quantity'] < min_q) | (df['quantity'] > max_q)
    bad_status = ~df['status'].isin(allowed_statuses)
    invalid = bad_quantity | bad_status

    for idx, row in df[invalid].iterrows():
        reasons = []
        if bad_quantity.loc[idx]:
            reasons.append('invalid_quantity')
        if bad_status.loc[idx]:
            reasons.append('invalid_status')
        quarantine.append(_quarantine_record(
            'orders', row['order_id'], ';'.join(reasons), row.to_dict(), run_id
        ))

    df = df[~invalid].copy()
    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = utc_now_iso()
    return df


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets plus a quarantine record set.

    Returns (staging_dict, quarantine_df).
    """
    quarantine_records = []

    customers = _stage_customers(raw_dir, run_id)
    products = _stage_products(raw_dir, run_id, quarantine_records)
    orders = _stage_orders(raw_dir, run_id, quarantine_records)

    quarantine_df = pd.DataFrame(quarantine_records)

    staging_dir = path_for('staging_dir')
    staging_dir.mkdir(parents=True, exist_ok=True)
    customers.to_parquet(staging_dir / 'customers.parquet', index=False)
    products.to_parquet(staging_dir / 'products.parquet', index=False)
    orders.to_parquet(staging_dir / 'orders.parquet', index=False)

    quarantine_dir = path_for('quarantine_dir')
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    if not quarantine_df.empty:
        quarantine_df.to_parquet(
            quarantine_dir / f'staging_quarantine_{run_id}.parquet', index=False
        )

    return {'customers': customers, 'products': products, 'orders': orders}, quarantine_df