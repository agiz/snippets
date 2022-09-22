from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2019, 6, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'pin_scraper_2',
    default_args=default_args,
    # schedule_interval=timedelta(minutes=1440),
    schedule_interval=timedelta(1),
    concurrency=1,    # prevent concurrent DAGs to execute same tasks
    max_active_runs=1 # prevent concurrent DAGs to execute same tasks
)

pg_db = Variable.get("postgres_db", deserialize_json=True)
sw_var = Variable.get("sw_var", deserialize_json=True)
promoter_mail_job = Variable.get("promoter_mail_job", deserialize_json=True)
mail_server = Variable.get("mail_server", deserialize_json=True)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag, trigger_rule='all_done')

t1 = DockerOperator(
    auto_remove=True,
    docker_url='tcp://socat:2375', # Set your docker URL (socat is from docker-compose service)
    image='agiz/sw-scraper',
    environment={
        'POSTGRES_DB_HOST': pg_db['host'],
        'POSTGRES_DB_PORT': pg_db['port'],
        'POSTGRES_DB_USER': pg_db['user'],
        'POSTGRES_DB_PASS': pg_db['pass'],
        'POSTGRES_DB_NAME': pg_db['name'],
        'START_DATE': '{{ ds }}',
        'PROXY': sw_var['proxy'],
    },
    task_id='sw_scraper_task',
    dag=dag,
    email_on_failure=True,
    email='adnalyticsairflow696969@robot.zapier.com',
)

t2 = DockerOperator(
    auto_remove=True,
    docker_url='tcp://socat:2375',
    image='agiz/language-detection',
    environment={
        'POSTGRES_DB_HOST': pg_db['host'],
        'POSTGRES_DB_PORT': pg_db['port'],
        'POSTGRES_DB_USER': pg_db['user'],
        'POSTGRES_DB_PASS': pg_db['pass'],
        'POSTGRES_DB_NAME': pg_db['name'],
        'START_DATE': '{{ ds }}',
    },
    task_id='language_detection_task',
    dag=dag,
    email_on_failure=True,
    email='adnalyticsairflow696969@robot.zapier.com',
)

t3 = DockerOperator(
    auto_remove=True,
    docker_url='tcp://socat:2375',
    image='agiz/shop-detection',
    environment={
        'POSTGRES_DB_HOST': pg_db['host'],
        'POSTGRES_DB_PORT': pg_db['port'],
        'POSTGRES_DB_USER': pg_db['user'],
        'POSTGRES_DB_PASS': pg_db['pass'],
        'POSTGRES_DB_NAME': pg_db['name'],
        'START_DATE': '{{ ds }}',
    },
    task_id='shop_test_task',
    dag=dag,
    email_on_failure=True,
    email='adnalyticsairflow696969@robot.zapier.com',
)

t4 = DockerOperator(
    auto_remove=True,
    docker_url='tcp://socat:2375',
    image='agiz/promoter-mail-job',
    environment={
        'POSTGRES_DB_HOST': pg_db['host'],
        'POSTGRES_DB_PORT': pg_db['port'],
        'POSTGRES_DB_USER': pg_db['user'],
        'POSTGRES_DB_PASS': pg_db['pass'],
        'POSTGRES_DB_NAME': pg_db['name'],
        'START_DATE': '{{ ds }}',
        'AMQP_SERVER': promoter_mail_job['amqp_server'],
        'AMQP_QUEUE': promoter_mail_job['amqp_queue'],
        'CRAWLED_AT_WINDOW': promoter_mail_job['crawled_at_window'],
        'CREATED_AT_WINDOW': promoter_mail_job['created_at_window'],
        'MAIL_HOST': mail_server['host'],
        'MAIL_PORT': mail_server['port'],
        'MAIL_USER': mail_server['user'],
        'MAIL_PASS': mail_server['pass'],
        'MAIL_FROM': mail_server['default_from_mail'],
        'MAIL_FROM_NAME': mail_server['default_from_name'],
    },
    task_id='promoter_mail_job_test_task',
    dag=dag,
    email_on_failure=True,
    email='adnalyticsairflow696969@robot.zapier.com',
)
for i in [83, 84, 81, 82, 61, 76, 77, 78, 79, 80, 69, 70, 71, 73, 74, 75, 51, 55, 56, 57, 58, 59, 64, 66, 67, 60, 63,]:
    task_name = 'profile-{}'.format(i)
    op = DockerOperator(
        auto_remove=True,
        docker_url='tcp://10.132.0.2:2375', # Set your docker URL
        image='agiz/datascraper',
        environment={
            'PROFILE_ID': str(i),
        },
        task_id=task_name,
        dag=dag,
        email_on_failure=True,
        email='adnalyticsairflow696969@robot.zapier.com')

    start.set_downstream(op)
    end.set_upstream(op)

end.set_downstream(t1)
end.set_downstream(t2)
end.set_downstream(t3)
t1.set_downstream(t4)
