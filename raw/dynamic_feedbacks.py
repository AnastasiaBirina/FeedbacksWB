import psycopg2
from datetime import date, timedelta

def delete_data(conn, cursor, dbname):
        try:
            cursor.execute(
                f"""
                DELETE FROM {dbname}
                WHERE date = %s
                """,
                (date.today(),)
            )
            conn.commit()
            print(f'Данные {dbname} за {date.today()} успешно удалены')
        except Exception as err:
            conn.rollback()
            print(f"Данные {dbname} за {date.today()} не удалены, ошибка: {err=}, {type(err)=}")
            
def dynamic_feedbacks_loader():
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="password", host="127.0.0.1")
    cursor = conn.cursor()
    # Удаление данных за сегодня на всякий случай
    delete_data(conn, cursor, 'public.dynamic_feedbacks')
    cursor.execute(
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
        (date.today(), date.today() - timedelta(days=1))
    )  
        
    # выполняем транзакцию
    try:
        conn.commit() 
        print(f'Данные dynamic_feedbacks за {date.today()} успешно загружены')
    except Exception as err:
        conn.rollback() 
        print(f"Данные dynamic_feedbacks за {date.today()} не загружены, ошибка на этапе транзакции: {err=}, {type(err)=}")
    
    cursor.close()
    conn.close() 