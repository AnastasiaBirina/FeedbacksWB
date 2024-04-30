import psycopg2
from datetime import date, timedelta
from airflow.hooks.base import BaseHook  
import logging


log = logging.getLogger(__name__)

class feedbacks_loader():
    def __init__(self, ds):
        self.connection = BaseHook.get_connection("POSTGRES_CONNECTION")
        self.conn = psycopg2.connect(dbname=self.connection.schema, 
                                     user=self.connection.login, 
                                     password=self.connection.password, 
                                     host=self.connection.host)
        self.cursor = self.conn.cursor()
        self.ds = ds
  
    def delete_data(self):
        try:
            self.cursor.execute(
                f"""
                DELETE FROM public.dynamic_feedbacks
                WHERE date = %s
                """,
                (self.ds,)
            )
            self.conn.commit()
            print(f'Данные public.dynamic_feedbacks за {self.ds} успешно удалены')
        except Exception as err:
            self.conn.rollback()
            print(f"Данные public.dynamic_feedbacks за {self.ds} не удалены, ошибка: {err=}, {type(err)=}")
            
    def load_data(self):
        self.cursor.execute(
            """
            with today_data AS (
                SELECT id, feedbacks
                FROM public.count_feedbacks
                WHERE date = %s
            ),
            yesterday_data AS (
                SELECT id, feedbacks
                FROM public.count_feedbacks
                WHERE date = %s)
            INSERT INTO dynamic_feedbacks (date, id, new_feedbacks)
            SELECT CURRENT_DATE, t.id, t.feedbacks - y.feedbacks AS new_feedbacks
            FROM today_data t inner join yesterday_data y using(id)
            WHERE t.feedbacks - y.feedbacks > 0;
            """,
            (self.ds, self.ds - timedelta(days=1))
        )  
            
        # выполняем транзакцию
        try:
            self.conn.commit() 
            print(f'Данные public.dynamic_feedbacks за {self.ds} успешно загружены')
        except Exception as err:
            self.conn.rollback() 
            print(f"Данные public.dynamic_feedbacks за {self.ds} не загружены, ошибка на этапе транзакции: {err=}, {type(err)=}")
        
        self.cursor.close()
        self.conn.close() 