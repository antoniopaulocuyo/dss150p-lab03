import logging
import pandas as pd
import psycopg
from src.config import DB
from src.common.audit import utc_now_iso

logger = logging.getLogger('dss150p.load')

UPSERT_SQL = """
INSERT INTO curated.sales_order_lines (
    order_id, customer_id, product_id, order_timestamp, customer_city, customer_tier,
    product_name, category, brand, quantity, unit_price, discount_pct,
    gross_amount, discount_amount, net_amount, status,
    source_updated_at, pipeline_run_id, processed_at_utc, record_hash
) VALUES (
    %(order_id)s, %(customer_id)s, %(product_id)s, %(order_timestamp)s, %(customer_city)s, %(customer_tier)s,
    %(product_name)s, %(category)s, %(brand)s, %(quantity)s, %(unit_price)s, %(discount_pct)s,
    %(gross_amount)s, %(discount_amount)s, %(net_amount)s, %(status)s,
    %(source_updated_at)s, %(pipeline_run_id)s, %(processed_at_utc)s, %(record_hash)s
)
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    product_id = EXCLUDED.product_id,
    order_timestamp = EXCLUDED.order_timestamp,
    customer_city = EXCLUDED.customer_city,
    customer_tier = EXCLUDED.customer_tier,
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    brand = EXCLUDED.brand,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    discount_pct = EXCLUDED.discount_pct,
    gross_amount = EXCLUDED.gross_amount,
    discount_amount = EXCLUDED.discount_amount,
    net_amount = EXCLUDED.net_amount,
    status = EXCLUDED.status,
    source_updated_at = EXCLUDED.source_updated_at,
    pipeline_run_id = EXCLUDED.pipeline_run_id,
    processed_at_utc = EXCLUDED.processed_at_utc,
    record_hash = EXCLUDED.record_hash
WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash
"""


def get_connection():
    return psycopg.connect(
        host=DB['host'], port=DB['port'], dbname=DB['dbname'],
        user=DB['user'], password=DB['password'],
    )


def upsert_curated(df: pd.DataFrame, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    order_id is the conflict key, so a rerun can never create a duplicate
    business key — Postgres either inserts a new row or updates the existing
    one. The WHERE clause on record_hash means a rerun with unchanged
    business content performs a true no-op update rather than rewriting
    every row every time.
    """
    if df.empty:
        logger.info('upsert_curated: nothing to load for run_id=%s', run_id)
        return 0

    records = df.to_dict(orient='records')
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, records)
        conn.commit()

    logger.info('upsert_curated: attempted %d row(s) for run_id=%s', len(records), run_id)
    return len(records)


def record_pipeline_run(run_id, status, rows_staging=None, rows_curated=None,
                         rows_quarantined=None, message=None,
                         started_at_utc=None, completed_at_utc=None):
    """Upsert one row per pipeline run into audit.pipeline_runs."""
    sql = """
    INSERT INTO audit.pipeline_runs (
        pipeline_run_id, started_at_utc, completed_at_utc, status,
        rows_staging, rows_curated, rows_quarantined, message
    ) VALUES (%(run_id)s, %(started_at)s, %(completed_at)s, %(status)s,
              %(rows_staging)s, %(rows_curated)s, %(rows_quarantined)s, %(message)s)
    ON CONFLICT (pipeline_run_id) DO UPDATE SET
        completed_at_utc = EXCLUDED.completed_at_utc,
        status = EXCLUDED.status,
        rows_staging = EXCLUDED.rows_staging,
        rows_curated = EXCLUDED.rows_curated,
        rows_quarantined = EXCLUDED.rows_quarantined,
        message = EXCLUDED.message
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {
                'run_id': run_id,
                'started_at': started_at_utc or utc_now_iso(),
                'completed_at': completed_at_utc,
                'status': status,
                'rows_staging': rows_staging,
                'rows_curated': rows_curated,
                'rows_quarantined': rows_quarantined,
                'message': message,
            })
        conn.commit()


def load_partition(df: pd.DataFrame, year: int, month: int, run_id: str) -> int:
    """Load a single year/month partition into curated.sales_order_lines and
    record the load in audit.partition_loads.

    Reuses the same rerun-safe UPSERT as upsert_curated (order_id conflict key,
    record_hash guard) so reloading the same partition never creates duplicate
    business rows. audit.partition_loads is itself upserted on partition_key,
    so re-running the same partition updates its row_count/loaded_at rather
    than creating a second audit row for the same partition.
    """
    if df.empty:
        logger.info('load_partition: nothing to load for %04d-%02d', year, month)
        return 0

    records = df.to_dict(orient='records')
    partition_key = f"{year:04d}-{month:02d}"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, records)
            cur.execute(
                """
                INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
                VALUES (%(partition_key)s, %(loaded_at)s, %(row_count)s, %(run_id)s)
                ON CONFLICT (partition_key) DO UPDATE SET
                    loaded_at_utc = EXCLUDED.loaded_at_utc,
                    row_count = EXCLUDED.row_count,
                    pipeline_run_id = EXCLUDED.pipeline_run_id
                """,
                {
                    'partition_key': partition_key,
                    'loaded_at': utc_now_iso(),
                    'row_count': len(records),
                    'run_id': run_id,
                },
            )
        conn.commit()

    logger.info('load_partition: loaded %d row(s) for partition %s (run_id=%s)',
                len(records), partition_key, run_id)
    return len(records)