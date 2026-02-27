from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator



dag = DAG(
    dag_id="30_days",
    start_date=datetime(2026, 2, 24),
    schedule="@daily",
    catchup=False,
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "echo 'START={{ macros.ds_add(ds, -30) }} END={{ ds }}' && "
        "curl -o /tmp/data/events_{{ ds }}.json http://event_api:5000/events?"
        "start_date={{ macros.ds_add(ds, -30) }}&end_date={{ ds }}"
    ),
    dag=dag,
)

def _calculate_stats(input_path, output_path):
    Path(output_path).parent.mkdir(exist_ok=True)
    
    events=pd.read_json(input_path)
    stats=events.groupby(["date","user"]).size().reset_index()
    
    stats.to_csv(output_path, index=False)
    
calculate_stats=PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/tmp/data/events_{{ds}}.json",
        "output_path": "/tmp/data/output_{{ds}}.csv",
    },
)

fetch_events >> calculate_stats