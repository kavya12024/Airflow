from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

dag = DAG(
    dag_id="six_tasks",
    start_date=datetime(2026, 2, 24),
    schedule="@daily",
    catchup=False,
)

A = BashOperator(
    task_id="task_A",
    bash_command="echo 'Task A'",
    dag=dag,
)

B = BashOperator(
    task_id="task_B",
    bash_command="echo 'Task B'",
    dag=dag,
)

C = BashOperator(
    task_id="task_C",
    bash_command="echo 'Task C'",
    dag=dag,
)

D = BashOperator(
    task_id="task_D",
    bash_command="echo 'Task D'",
    dag=dag,
)

E = BashOperator(
    task_id="task_E",
    bash_command="echo 'Task E'",
    dag=dag,
)

F = BashOperator(
    task_id="task_F",
    bash_command="echo 'Task F'",
    dag=dag,
)

G = BashOperator(
    task_id="task_G",
    bash_command="echo 'Task G'",
    dag=dag,
)

H = BashOperator(
    task_id="task_H",
    bash_command="echo 'Task H'",
    dag=dag,
)

A >> [B, C, G]
B >> D
C >> E
[D, E, H] >> F
G >> H
