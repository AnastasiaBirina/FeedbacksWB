# dict_keys(['time1', 'time2', 'wh', 'dtype', 'dist', 'id', 'root', 'kindId', 'brand', 'brandId', 'siteBrandId', 'colors', 'subjectId', 'subjectParentId', 'name', 'supplier', 'supplierId', 'supplierRating', 'supplierFlags', 'priceU', 'salePriceU', 'sale', 'logisticsCost', 'returnCost', 'diffPrice', 'saleConditions', 'pics', 'rating', 'reviewRating', 'feedbacks', 'panelPromoId', 'promoTextCard', 'promoTextCat', 'volume', 'viewFlags', 'sizes', 'log', 'logs'])

# IMPORT 
import requests
import json
import psycopg2
from datetime import date, timedelta
from soup2dict import convert
from bs4 import BeautifulSoup

# VARIABLE DEFINITION
query = 'Наклейки для творчества'
url = 'https://search.wb.ru/exactmatch/ru/common/v4/search?TestGroup=no_test&TestID=no_test&appType=1&curr=rub&dest=-1257786&query=' + query + '&resultset=catalog&sort=popular&spp=99&suppressSpellcheck=false'

# # get data from wb
# class db_execution():
#     def __init__():
#         pass
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

def insert_data_count_feedbacks(clear_data):
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="password", host="127.0.0.1")
    cursor = conn.cursor()

    # Удаление данных за сегодня на всякий случай
    delete_data(conn, cursor, 'public.count_feedbacks')

    # Запись данных за сегодня
    for product in clear_data:
        cursor.execute(
            """
            INSERT INTO count_feedbacks (date, id, salePriceU, feedbacks)
            VALUES (%s, %s, %s, %s)
            """,
            (date.today(), product["id"], product["priceU"], product["feedbacks"])
        )
        
    # выполняем транзакцию
    try:
        conn.commit() 
        print(f'Данные count_feedbacks за {date.today()} успешно загружены')
    except Exception as err:
        conn.rollback() 
        print(f"Данные count_feedbacks за {date.today()} не загружены, ошибка на этапе транзакции: {err=}, {type(err)=}")
     
    cursor.close()
    conn.close()  


def insert_data_dynamic_feedbacks():
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

def main():
    tries_max = 20
    tries_cur = 0
    clear_data = get_data(url)

    # Пытаемся забрать данные
    while not clear_data and tries_cur < tries_max:
        clear_data = get_data(url)
        tries_cur += 1

    # Если данные так и не забрали, пишем об этом. Иначе идет обработка данных
    print(f'Попыток: {tries_cur}')
    if not clear_data:
        print('Не получилось получить данные (больше 20 попыток)')
    else:
        insert_data_count_feedbacks(clear_data)
        insert_data_dynamic_feedbacks()
        

def get_data(request_url):
    response = requests.get(request_url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        clear_data = json.loads(convert(soup)['navigablestring'][0])['data']
        if 'total' in clear_data.keys():
            return clear_data['products']
        else:
            return False
    else:
        print(f'Ошибка: {response.status_code} - {response.text}') 
        return False 

main()



    
    