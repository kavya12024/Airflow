from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="fan_in_out" ,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)as dag:
    start=EmptyOperator(task_id="start")

    fetch_sales=EmptyOperator(task_id="fetch_sales")
    clean_sales=EmptyOperator(task_id="clean_sales")

    fetch_weather=EmptyOperator(task_id="fetch_weather")
    clean_weather=EmptyOperator(task_id="clean_weather")

    join_datset=EmptyOperator(task_id="join_dataset")
    train_model=EmptyOperator(task_id="train_model")
    deploy_model=EmptyOperator(task_id="deploy_model")

    start>>[fetch_sales,fetch_weather]
    fetch_sales>>clean_sales
    fetch_weather>>clean_weather
    [clean_sales,clean_weather]>>join_datset>>train_model>>deploy_model