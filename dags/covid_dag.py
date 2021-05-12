from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from covid_etl import sql2figure, covid_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'covid_dag',
    default_args=default_args,
    description='Daily download covid statistics from GitHub!',
    schedule_interval=timedelta(days=1),
)

# def just_a_function():
#     print("I'm going to show you something :)")

install = BashOperator(
    task_id="install requirement",
    bash_command='python -m pip install -r requirement.txt',
)

download = PythonOperator(
    task_id='download data to database',
    python_callable=covid_etl,
    op_args=['India', '01-01-2021'],
    dag=dag,
)
plot_save = PythonOperator(
    task_id='plot and save',
    python_callable=sql2figure,
    op_args=['India', '01-01-2021'],
    dag=dag,
)


install >> download >> plot_save