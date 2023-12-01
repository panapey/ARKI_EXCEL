import glob
from datetime import datetime

import mysql.connector
import pandas as pd

db_config = {
    'host': 'HOST',
    'port': PORT,
    'user': 'USER',
    'password': 'PASSWORD',
    'db': 'DB',
}

current_date = datetime.now().strftime('%Y-%m-%d')

cnx = mysql.connector.connect(**db_config)

cursor = cnx.cursor()
cnx = mysql.connector.connect(**db_config)

# Создаем курсор для выполнения SQL-запросов
cursor = cnx.cursor()


def query_and_search():
    # Считываем данные из файла Excel
    df = pd.read_excel('ARKI.xlsx', engine='openpyxl')

    # Получаем данные из колонки group_id
    group_ids = df['group_id'].tolist()

    # Словарь с данными для подключения к базе данных

    # Создаем соединение с базой данных

    # Для каждого group_id в списке
    for group_id in group_ids:
        # Создаем SQL-запрос
        query = f"""
    SELECT dev.DEVICE_ID, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
                            FROM powerdb.devices dev
                            LEFT JOIN powerdb.adapters a 
                            ON dev.DEVICE_ID = a.ID_DEVICE
                            LEFT JOIN powerdb.adapter_parameters ap 
                            ON a.ID_ADAPTER = ap.ID_ADAPTER
                            LEFT OUTER JOIN powerdb.records r 
                            ON a.ID_ADAPTER = r.ID_ADAPTER
                            LEFT OUTER JOIN powerdb.data d 
                            ON ap.ID_PARAMETER = d.ID_PARAMETER and r.ID_RECORD = d.ID_RECORD
                            RIGHT JOIN powerdb.groups g
        on  dev.DEVICE_ID = g.DEVICE_ID
                            WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) 

                            and ap.PARAMETER_NAME like "Тепловая энергия%"
                            and g.ID_OWNER = {group_id}
                            ORDER BY dev.DEVICE_NAME;
    """

        # Выполняем SQL-запрос
        cursor.execute(query)

        # Получаем все строки
        rows = cursor.fetchall()

        # Выводим результаты
        for row in rows:
            print(row)

    # Создаем SQL-запрос для получения device_id
    query_device_id = "SELECT DEVICE_ID FROM powerdb.devices"

    # Выполняем SQL-запрос
    cursor.execute(query_device_id)

    # Получаем все строки
    device_ids = cursor.fetchall()

    # Закрываем соединение с базой данных
    cnx.close()

    # Поиск device_id в txt файлах
    for device_id in device_ids:
        device_id = device_id[0]  # Извлекаем device_id из кортежа
        for file in glob.glob("*.txt"):  # Перебираем все txt файлы в текущей директории
            with open(file, 'r') as f:
                if str(device_id) in f.read():
                    print(f"Device ID {device_id} found in {file}")


query_and_search()
