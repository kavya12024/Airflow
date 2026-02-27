from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import get_current_context
from airflow.utils import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
PythonOperator,
BranchPythonOperator,
)
START_DATE=timezone.datetime(2026, 2, 23)
ERP_CHANGE_DATE=timezone.datetime(2026, 2, 25)

def _pick_erp_system():
    context=get_current_context()
    logical_date=context["logical_date"]
    if logical_date<ERP_CHANGE_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"
def _fetch_sales_old(**context):
    print("Fetching old sales data")
def _fetch_sales_new(**context):
    print("Fetching new sales data")
def _clean_sales(**context):
    print("Cleaning  old sales data")

def _clean_sales_new(**context):
    print("Cleaning  new sales data")
def _fetch_weather_old(**context):
    print("Fetching  old weather data")    
def _fetch_weather_new(**context):
    print("Fetching  new weather data")     
with DAG(
    dag_id="branching_day",
    start_date=START_DATE,
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },  
    tags=["branching", "example"], 
) as dag:
    start=EmptyOperator(task_id="start")
    pick_erp_system=BranchPythonOperator(
        task_id="pick_erp_system",
        python_callable=_pick_erp_system,
    )
    fetch_sales_old=PythonOperator(
        task_id="fetch_sales_old",
        python_callable=_fetch_sales_old,
    )
    fetch_sales_new=PythonOperator(
        task_id="fetch_sales_new",
        python_callable=_fetch_sales_new,
    )
    clean_sales_old=PythonOperator(
        task_id="clean_sales_old",
        python_callable=_clean_sales,
    )
    clean_sales_new=PythonOperator(
        task_id="clean_sales_new",
        python_callable=_clean_sales_new,
    )
      
    fetch_weather=EmptyOperator(task_id="fetch_weather")
    clean_weather=EmptyOperator(task_id="clean_weather")


    join_datset=EmptyOperator(task_id="join_dataset",trigger_rule="none_failed")
    train_model=EmptyOperator(task_id="train_model")
    deploy_model=EmptyOperator(task_id="deploy_model")


    start>>[pick_erp_system,fetch_weather]
    pick_erp_system>>[fetch_sales_old,fetch_sales_new]
    fetch_sales_old>>clean_sales_old
    fetch_sales_new>>clean_sales_new
    fetch_weather>>clean_weather
    [clean_sales_old,clean_sales_new,clean_weather]>>join_datset>>train_model>>deploy_model