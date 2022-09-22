from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    "start_date": datetime(2019, 6, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='pinterest_saves_test',
    default_args=default_args,
    schedule_interval=timedelta(1),
    # dagrun_timeout=timedelta(minutes=720),
    concurrency=1,     # prevent concurrent DAGs to execute same tasks
    max_active_runs=1, # prevent concurrent DAGs to execute same tasks
    catchup=False,     # prevent backfills
)

# [START bash operator]
t1 = BashOperator(
    task_id='pinterest_saves_test_task',
    bash_command='Starting ssh task',
    dag=dag,
)
# [END bash operator]

t2_bash = """
echo 'Hello World'
docker-compose -f /home/ziga_zupanec/Documents/projects/save-detection-2/docker-compose.yml ps
docker-compose -f /home/ziga_zupanec/Documents/projects/save-detection-2/docker-compose.yml down
docker-compose -f /home/ziga_zupanec/Documents/projects/save-detection-2/docker-compose.yml up --build --abort-on-container-exit
docker-compose -f /home/ziga_zupanec/Documents/projects/save-detection-2/docker-compose.yml down
docker-compose -f /home/ziga_zupanec/Documents/projects/save-detection-2/docker-compose.yml ps
"""

t2 = SSHOperator(
    ssh_conn_id='ssh_instance_2',
    task_id='test_ssh_operator_2',
    command=t2_bash,
    dag=dag)

t1.set_downstream(t2)
