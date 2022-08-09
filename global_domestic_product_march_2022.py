import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

"""This is an example of ETL-pipeline that loads csv file into PostgreSQL"""

default_args = {
    'owner':'airflow',
    'start_date':datetime(year=2022, month=8, day=8),
    'depends_on_past':False,
}

def process_dataset():
    df = pd.read_csv('/absolute_path/Gross-domestic-product-March-2022.csv', delimiter=',')
    df.columns = ['level','description','seriesrefsndq','quarter','weight','amount']
    df.to_csv('/absolute_path/gdp_march_2022.csv', sep=',', index=False)

with DAG(
    dag_id='gdp_march_2022_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    task_process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_dataset,
        dag=dag
    )

    task_create_table = BashOperator(
        task_id='create_table',
        bash_command=(
            'psql -d gdp2022 -U username -c "'
            'CREATE TABLE gdp_march_2022 ( \
                level varchar(26), \
                description varchar(75), \
                seriesrefsndq varchar(15), \
                quarter varchar(6), \
                weight real, \
                amount real \
                );'
        )
    )

    task_load_data = BashOperator(
        task_id='load_data',
        bash_command=(
            'psql -d gdp2022 -U username -c "'
            'COPY gdp_march_2022(level, description, seriesrefsndq, quarter, weight, amount)'
            "FROM '/absolute_path/gdp_march_2022.csv' "
            "DELIMITER ',' "
            'CSV HEADER"'
        )
    )
    task_process_data  >> task_create_table >> task_load_data