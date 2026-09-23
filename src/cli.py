import argparse
import logging
import pandas as pd
import os
from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id, utc_now_iso
from src.common.errors import PipelineStageError
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load.postgres import upsert_curated, record_pipeline_run, load_partition
from src.validate.quality import validate_curated

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger('dss150p.cli')


def _run_stage(stage_name: str, run_id: str, func, *args, **kwargs):
    logger.info('stage_start stage=%s run_id=%s', stage_name, run_id)
    try:
        result = func(*args, **kwargs)
    except Exception as exc:
        logger.error('stage_failed stage=%s run_id=%s error=%s', stage_name, run_id, exc)
        raise PipelineStageError(stage_name, run_id, str(exc)) from exc
    logger.info('stage_complete stage=%s run_id=%s', stage_name, run_id)
    return result


def _load_curated_from_disk():
    curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
    if not curated_path.exists():
        raise FileNotFoundError(
            f"No curated dataset at {curated_path}. Run 'transform' or 'run-all' first."
        )
    return pd.read_parquet(curated_path)


def cmd_extract(run_id):
    raw_dir = _run_stage('extract', run_id, extract_sources, run_id)
    print('raw_dir=', raw_dir)
    return raw_dir


def cmd_transform(run_id, raw_dir=None):
    if raw_dir is None:
        raw_dir = cmd_extract(run_id)
    staging, staging_q = _run_stage('staging', run_id, build_staging, raw_dir, run_id)
    curated, curated_q = _run_stage('curated', run_id, build_curated, staging, run_id)
    print('staged rows:', {k: len(v) for k, v in staging.items()}, '| staging quarantined:', len(staging_q))
    print('curated rows:', len(curated), '| curated quarantined:', len(curated_q))
    return curated


def cmd_load(run_id):
    curated = _load_curated_from_disk()
    loaded = _run_stage('load', run_id, upsert_curated, curated, run_id)
    print('rows upserted (attempted):', loaded)
    return loaded


def cmd_validate(run_id):
    curated = _load_curated_from_disk()
    errors = _run_stage('validate', run_id, validate_curated, curated)
    if errors:
        print(f'{len(errors)} validation issue(s):')
        for e in errors:
            print(' -', e)
    else:
        print('validation passed: no issues found')
    return errors


def cmd_run_all(run_id):
    started = utc_now_iso()
    try:
        raw_dir = cmd_extract(run_id)
        curated = cmd_transform(run_id, raw_dir)
        upsert_curated(curated, run_id)
        errors = cmd_validate(run_id)
        record_pipeline_run(
            run_id, status='SUCCEEDED' if not errors else 'SUCCEEDED_WITH_WARNINGS',
            rows_curated=len(curated), message='run-all completed',
            started_at_utc=started, completed_at_utc=utc_now_iso(),
        )
    except PipelineStageError as exc:
        record_pipeline_run(
            run_id, status='FAILED', message=str(exc),
            started_at_utc=started, completed_at_utc=utc_now_iso(),
        )
        raise

def cmd_load_partition(run_id, year, month):
    partition_path = path_for('partition_dir') / f'order_year={year}' / f'order_month={month}'
    if not partition_path.exists():
        raise FileNotFoundError(
            f"No partition found at {partition_path}. Run the benchmark/partition step first."
        )
    df = pd.read_parquet(partition_path)
    loaded = _run_stage('load_partition', run_id, load_partition, df, year, month, run_id)
    print(f'rows loaded for partition {year}-{month:02d}:', loaded)
    return loaded

def cmd_benchmark(run_id, repeats):
    from src.benchmark.storage import run_benchmark
    curated_path = path_for('curated_dir') / 'sales_order_lines.parquet'
    if not curated_path.exists():
        raise FileNotFoundError(
            f"No curated dataset at {curated_path}. Run 'transform' or 'run-all' first."
        )
    results = _run_stage(
        'benchmark', run_id, run_benchmark, curated_path, path_for('benchmark_dir'), repeats
    )
    print(results.to_string())
    return results


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    run_id = os.environ.get('PIPELINE_RUN_ID') or new_run_id()
    try:
        if args.command == 'extract':
            cmd_extract(run_id)
        elif args.command == 'transform':
            cmd_transform(run_id)
        elif args.command == 'load':
            cmd_load(run_id)
        elif args.command == 'validate':
            cmd_validate(run_id)
        elif args.command == 'run-all':
            cmd_run_all(run_id)
        elif args.command == 'load-partition':
            cmd_load_partition(run_id, args.year, args.month)
        elif args.command == 'benchmark':
            cmd_benchmark(run_id, args.repeats)
    except PipelineStageError as exc:
        logger.error('Pipeline run failed: %s', exc)
        raise SystemExit(1)


if __name__ == '__main__':
    main()