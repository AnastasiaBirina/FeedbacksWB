from datetime import datetime, timedelta
from raw.wb_postgres import wb_postgres_loader
from raw.dynamic_feedbacks import dynamic_feedbacks_loader

from airflow import DAG
import logging
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


log = logging.getLogger(__name__)
query = 'Наклейки для творчества'
url = 'https://search.wb.ru/exactmatch/ru/common/v4/search?TestGroup=no_test&TestID=no_test&appType=1&curr=rub&dest=-1257786&query=' + query + '&resultset=catalog&sort=popular&spp=99&suppressSpellcheck=false'


with DAG(
    dag_id="Feedbacks_WB",
    schedule='0 19 * * *',
    start_date=datetime(2024, 4, 30),
    catchup=False,
    tags=["postgres", "python"]
):
    
    def wb_data_to_postgres_task():
        wb_postgres_loader()

    def insert_data_dynamic_feedbacks_task():
        dynamic_feedbacks_loader()

    wb_data_to_postgres = PythonOperator(
        task_id='wb_data_to_postgres_task',
        python_callable = wb_data_to_postgres_task
    )
    
    insert_data_dynamic_feedbacks = PythonOperator(
        task_id='insert_data_dynamic_feedbacks_task',
        python_callable = insert_data_dynamic_feedbacks_task
    )

    wb_data_to_postgres >> insert_data_dynamic_feedbacks
