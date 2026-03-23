from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "retries": 2,
    "email_on_failure": True,
    "email": ["yrohitha996@gmail.com"]
}

with DAG(
    dag_id="dbt_daily_pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd ../ecomm-analysis && dbt deps"
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd ../ecomm-analysis && dbt seed --target dev"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd ../ecomm-analysis && dbt run --target dev --select state:modified+"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd ../ecomm-analysis && dbt test --target dev"
    )

    # 🔗 Dependency Order
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test
