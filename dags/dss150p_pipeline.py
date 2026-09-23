from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'


def failure_callback(context):
    """Write concise, diagnosable failure context: which task, which DAG run,
    and the underlying exception. Printed to task logs (visible in the UI)
    rather than swallowed, so a failed run is diagnosable without re-running.
    """
    ti = context['task_instance']
    exception = context.get('exception')
    print(
        f"TASK FAILED | dag_id={ti.dag_id} task_id={ti.task_id} "
        f"run_id={context['run_id']} try={ti.try_number} "
        f"execution_date={context['ts']} error={exception}"
    )


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
    'on_failure_callback': failure_callback,
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    # Daily at 02:00 UTC: source systems export overnight batches; running
    # after midnight but well before business hours gives a full day's data
    # a chance to land upstream while still finishing before analysts need
    # the curated tables each morning.
    schedule='0 2 * * *',
    # catchup=False: this pipeline reflects the *current* state of the source
    # files each run, not a historical delta per calendar day. Backfilling by
    # re-running past dates would just reprocess the same current snapshot
    # under a different logical date, which is misleading rather than useful.
    # Historical backfills should be handled deliberately (see backfill notes),
    # not automatically via catchup.
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )

    # run_mode drives which load happens: a full curated reload, or just one
    # selected partition. This is an orchestration decision (which reusable
    # CLI command to invoke), not business logic — the actual load/partition
    # rules still live entirely in src/, satisfying the "DAG calls reusable
    # modules/CLI commands" requirement.
    load = BashOperator(
        task_id='load',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli '
            '{% if params.run_mode == "partition" %}'
            'load-partition --year {{ params.year }} --month {{ params.month }}'
            '{% else %}'
            'load'
            '{% endif %}'
        ),
    )

    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

    extract >> transform >> load >> validate