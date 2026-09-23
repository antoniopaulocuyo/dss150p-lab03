import csv
import json
import statistics
import time
import warnings
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import SETTINGS
from src.load.postgres import get_connection


def _median_time(func, repeats: int):
    """Run func() `repeats` times, return (median_seconds, last_result)."""
    times = []
    result = None
    for _ in range(repeats):
        start = time.perf_counter()
        result = func()
        times.append(time.perf_counter() - start)
    return statistics.median(times), result


def _write_csv(df: pd.DataFrame, path: Path) -> float:
    start = time.perf_counter()
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)
    return time.perf_counter() - start


def _write_jsonl(df: pd.DataFrame, path: Path) -> float:
    start = time.perf_counter()
    with path.open('w', encoding='utf-8') as f:
        for record in df.to_dict(orient='records'):
            f.write(json.dumps(record, default=str) + '\n')
    return time.perf_counter() - start


def _write_parquet(df: pd.DataFrame, path: Path) -> float:
    start = time.perf_counter()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression='snappy')
    return time.perf_counter() - start


def _read_csv_full(path):
    return pd.read_csv(path)


def _read_csv_filtered(path, status):
    return _read_csv_full(path).loc[lambda d: d['status'] == status]


def _read_jsonl_full(path):
    return pd.read_json(path, lines=True)


def _read_jsonl_filtered(path, status):
    return _read_jsonl_full(path).loc[lambda d: d['status'] == status]


def _read_parquet_full(path):
    return pd.read_parquet(path)


def _read_parquet_filtered(path, status):
    return pd.read_parquet(path, filters=[('status', '==', status)])


def _pg_full_read():
    sql = "SELECT * FROM curated.sales_order_lines"
    with get_connection() as conn:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return pd.read_sql(sql, conn)


def _pg_filtered_read(status):
    sql = "SELECT * FROM curated.sales_order_lines WHERE status = %(status)s"
    with get_connection() as conn:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return pd.read_sql(sql, conn, params={'status': status})


def _pg_table_size_bytes():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('curated.sales_order_lines')")
            return cur.fetchone()[0]


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL.

    Measures write time, full-read time (median of `repeats` runs), filtered-read
    time (status filter, median of `repeats` runs), row counts, and file/table size.
    Writes results to output_dir/benchmark_results.csv and returns the results DataFrame.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(curated_path)
    filter_status = SETTINGS['storage_benchmark']['filter_status']

    csv_path = output_dir / 'sales_order_lines.csv'
    jsonl_path = output_dir / 'sales_order_lines.jsonl'
    parquet_path = output_dir / 'sales_order_lines.parquet'

    rows = []

    # --- CSV ---
    write_time = _write_csv(df, csv_path)
    full_time, full_df = _median_time(lambda: _read_csv_full(csv_path), repeats)
    filtered_time, filtered_df = _median_time(
        lambda: _read_csv_filtered(csv_path, filter_status), repeats
    )
    rows.append({
        'format': 'csv',
        'size_bytes': csv_path.stat().st_size,
        'write_time_sec': write_time,
        'full_read_median_sec': full_time,
        'full_read_rows': len(full_df),
        'filtered_read_median_sec': filtered_time,
        'filtered_read_rows': len(filtered_df),
    })

    # --- JSON Lines ---
    write_time = _write_jsonl(df, jsonl_path)
    full_time, full_df = _median_time(lambda: _read_jsonl_full(jsonl_path), repeats)
    filtered_time, filtered_df = _median_time(
        lambda: _read_jsonl_filtered(jsonl_path, filter_status), repeats
    )
    rows.append({
        'format': 'jsonl',
        'size_bytes': jsonl_path.stat().st_size,
        'write_time_sec': write_time,
        'full_read_median_sec': full_time,
        'full_read_rows': len(full_df),
        'filtered_read_median_sec': filtered_time,
        'filtered_read_rows': len(filtered_df),
    })

    # --- Parquet (snappy) ---
    write_time = _write_parquet(df, parquet_path)
    full_time, full_df = _median_time(lambda: _read_parquet_full(parquet_path), repeats)
    filtered_time, filtered_df = _median_time(
        lambda: _read_parquet_filtered(parquet_path, filter_status), repeats
    )
    rows.append({
        'format': 'parquet_snappy',
        'size_bytes': parquet_path.stat().st_size,
        'write_time_sec': write_time,
        'full_read_median_sec': full_time,
        'full_read_rows': len(full_df),
        'filtered_read_median_sec': filtered_time,
        'filtered_read_rows': len(filtered_df),
    })

    # --- PostgreSQL (already loaded in Goal 2; measure query time + server-side size) ---
    full_time, full_df = _median_time(_pg_full_read, repeats)
    filtered_time, filtered_df = _median_time(lambda: _pg_filtered_read(filter_status), repeats)
    rows.append({
        'format': 'postgresql',
        'size_bytes': _pg_table_size_bytes(),
        'write_time_sec': None,  # already loaded in Goal 2; not a comparable fresh write
        'full_read_median_sec': full_time,
        'full_read_rows': len(full_df),
        'filtered_read_median_sec': filtered_time,
        'filtered_read_rows': len(filtered_df),
    })

    results_df = pd.DataFrame(rows)
    results_df.to_csv(output_dir / 'benchmark_results.csv', index=False)
    return results_df


def write_partitioned_parquet(df, output_dir):
    """Write Parquet partitioned by order_year/order_month derived from order_timestamp."""
    output_dir = Path(output_dir)
    df = df.copy()
    ts = pd.to_datetime(df['order_timestamp'], utc=True)
    df['order_year'] = ts.dt.year
    df['order_month'] = ts.dt.month

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=SETTINGS['storage_benchmark']['partition_columns'],
    )
    return output_dir