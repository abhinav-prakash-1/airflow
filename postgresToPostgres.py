from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators import PythonOperator


def create_table(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_lab')
    sql = '''create table humanresources.airflow_dept as select * from humanresources.department where 1 = 2;'''
    pg_hook.run(sql)


def load_data(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_lab')
    sql_load = '''insert into humanresources.airflow_dept (select * from humanresources.department);'''
    pg_hook.run(sql_load)



dag_params = {
    'dag_id': 'PostgresOperator_dag',
    'start_date': datetime(2019, 10, 7)
}

dag = DAG('postgresToPostgres', default_args=dag_params, schedule_interval='*/2 * * * *')

create_table_task = \
    PythonOperator(task_id='create_table',
                   provide_context=True,
                   python_callable=create_table,
                   dag=dag)

load_data_task = \
    PythonOperator(task_id='load_data',
                   provide_context=True,
                   python_callable=load_data,
                   dag=dag)
