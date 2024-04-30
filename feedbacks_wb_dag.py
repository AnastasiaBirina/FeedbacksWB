from datetime import datetime, timedelta
from feedbacks_wb.raw.wb_postgres import WB_loader
from feedbacks_wb.raw.dynamic_feedbacks import feedbacks_loader
from datetime import datetime, timedelta


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
    
    def wb_data_to_postgres_task(ds):
        # ds_date = datetime.strptime(ds, '%Y-%m-%d')
        ds_date = datetime(2024, 5, 1)
        WB_data = WB_loader(ds_date)
        WB_data.delete_data()
        WB_data.wb_postgres_loader()

    def insert_data_dynamic_feedbacks_task(ds):
        # ds_date = datetime.strptime(ds, '%Y-%m-%d')
        ds_date = datetime(2024, 5, 1)
        dynamic_feedbacks = feedbacks_loader(ds_date)
        dynamic_feedbacks.delete_data()
        dynamic_feedbacks.load_data()


    wb_data_to_postgres = PythonOperator(
        task_id='wb_data_to_postgres_task',
        python_callable = wb_data_to_postgres_task,
        op_kwargs={
            "current_dag_run_date": "{{ds}}"
        }
    )
    
    insert_data_dynamic_feedbacks = PythonOperator(
        task_id='insert_data_dynamic_feedbacks_task',
        python_callable = insert_data_dynamic_feedbacks_task,
        op_kwargs={
            "current_dag_run_date": "{{ds}}"
        }
    )
    # insert_data_dynamic_feedbacks = PostgresOperator(
    #     task_id="predelete_today_data",
    #     postgres_conn_id="POSTGRES_CONNECTION",
    #     sql='feedbacks_wb.sql.dynamic_feedbacks.sql'
    # )

    wb_data_to_postgres >> insert_data_dynamic_feedbacks
