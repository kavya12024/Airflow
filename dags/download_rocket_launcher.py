import json
import pathlib
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="download_rocket_launch",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)


download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)


def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    with open("/tmp/launches.json") as f:
        launches = json.load(f)

    if "results" not in launches:
        print("No 'results' key found in API response.")
        print("Response:", launches)
        return

    for launch in launches["results"]:
        image_url = launch.get("image")

        if not image_url:
            continue

        try:
            response = requests.get(image_url, timeout=10)

            if response.status_code != 200:
                continue

            image_filename = image_url.split("/")[-1]
            target_file = f"/tmp/images/{image_filename}"

            with open(target_file, "wb") as img:
                img.write(response.content)

            print(f"Downloaded {image_url}")

        except Exception as e:
            print(f"Skipping {image_url} because {e}")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)


notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images"',
    dag=dag,
)


download_launches >> get_pictures >> notify