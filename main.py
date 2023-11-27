import re

import chardet
import pandas as pd
import pymysql

# Ваши данные для подключения к базе данных
db_config = {
    'host': 'HOST',
    'port': PORT,
    'user': 'USER',
    'password': 'PASSWORD',
    'db': 'DB',
}


def format_address(address):
    try:
        # Удаляем пробелы по краям
        address = address.strip()
        # Заменяем "г." на "г."
        address = re.sub(r"г\.", "г.", address)
        # Заменяем "мкр." на "мкр."
        address = re.sub(r"мкр\.", "мкр.", address)
        # Заменяем "д." на "д."
        address = re.sub(r"д\.", "д.", address)
        # Заменяем "с." на "с."
        address = re.sub(r"с\.", "с.", address)
        # Заменяем "п." на "п."
        address = re.sub(r"п\.", "п.", address)
        # Заменяем "тер." на "тер."
        address = re.sub(r"тер\.", "тер.", address)
        # Заменяем "ул." на ""
        address = re.sub(r"ул\.", "", address)
        # Заменяем "пер." на "пер."
        address = re.sub(r"пер\.", "пер.", address)
        # Заменяем "пр-кт." на "пр-кт."
        address = re.sub(r"пр-кт\.", "пр-кт.", address)
        # Заменяем "проезд." на "проезд."
        address = re.sub(r"проезд\.", "проезд", address)
        # Заменяем "аллея." на "аллея"
        address = re.sub(r"аллея\.", "аллея", address)
        # Заменяем "к." на "к"
        address = re.sub(r"к\.", "к", address)
        # Удаляем все лишние пробелы
        address = re.sub(r"\s+", " ", address)
        # Удаляем пробел перед "к"
        address = re.sub(r" к", "к", address)
        # Удаляем запятую между городом и улицей
        address = re.sub(r", ", " ", address)
        # Приводим адрес к шаблону "г. Город, д. Деревня, ул. Улица, д. ДомКорпус"
        address = re.sub(
            r"(г\.\s*)(\w+\\,)(,\s*б-р\.\s*|,\s*д\.\s*|,\s*ул\.\s*)?(\w+)?(,\s*д\.\s*)(\d+\w*)",
            r"\1\2\3\4\5\6", address)
        # Удаляем "д." если за ним следуют цифры
        address = re.sub(r"д\.\s*(\d+)", r"\1", address)
    except Exception as e:
        address = ""
    return address


rawdata = open('addresses.xlsx', 'rb').read()
result = chardet.detect(rawdata)
encoding = result['encoding']

# Считываем CSV-файл
df = pd.read_excel('addresses.xlsx')
print(df.columns)

# Применяем функцию format_address к каждому адресу в столбце 'Address'
df['Address'] = df['Address'].apply(format_address)

# Создаем соединение с базой данных MySQL
conn = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'],
                       db=db_config['db'])

# Создаем курсор для выполнения SQL-запросов
cursor = conn.cursor()

# Создаем новый столбец в DataFrame для хранения результатов запроса
df['QueryResults'] = ''

# Проходим по каждому адресу в DataFrame
for index, row in df.iterrows():
    # Создаем SQL-запрос с текущим адресом
    query = f"SELECT GROUP_NAME FROM `groups` g WHERE g.GROUP_NAME LIKE '%{row['Address']}%'"

    # Выполняем SQL-запрос
    cursor.execute(query)

    # Получаем все строки из результата запроса
    rows = cursor.fetchall()

    # Записываем результаты запроса в новый столбец
    df.at[index, 'QueryResults'] = str(rows)

# Закрываем соединение с базой данных
conn.close()

# Сохраняем результат в новый CSV-файл
df.to_excel('./addresses.xlsx')
