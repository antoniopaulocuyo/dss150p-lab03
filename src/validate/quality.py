from src.config import SETTINGS


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors."""
    errors = []

    if df['order_id'].isna().any():
        errors.append('order_id contains null values')

    dup = df['order_id'].duplicated().sum()
    if dup:
        errors.append(f'{dup} duplicate order_id value(s) found')

    bad_qty = ((df['quantity'] < 1) | (df['quantity'] > 20)).sum()
    if bad_qty:
        errors.append(f'{bad_qty} row(s) with quantity outside 1..20')

    neg_amounts = ((df['gross_amount'] < 0) | (df['discount_amount'] < 0) | (df['net_amount'] < 0)).sum()
    if neg_amounts:
        errors.append(f'{neg_amounts} row(s) with negative monetary amount')

    allowed = set(SETTINGS['quality']['allowed_order_statuses'])
    bad_status = (~df['status'].isin(allowed)).sum()
    if bad_status:
        errors.append(f'{bad_status} row(s) with disallowed status')

    for col in ['source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash']:
        if df[col].isna().any():
            errors.append(f'{col} contains null values')

    return errors