import re

import chardet
import pandas as pd


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
        address = re.sub(r"проезд\.", "проезд.", address)
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
            r"(г\.\s*)(\w+)(,\s*б-р\.\s*|,\s*д\.\s*|,\s*ул\.\s*)?(\w+)?(,\s*д\.\s*)(\d+\w*)",
            r"\1\2\3\4\5\6", address)
        # Заменяем второе упоминание "д." на пустую строку, если оно следует сразу после первого упоминания "д."
        address = re.sub(r"(д\.\s*\d+\w*),\s*д\.", r"\1,", address)
    except Exception as e:
        address = ""
    return address


rawdata = open('adresses.csv', 'rb').read()
result = chardet.detect(rawdata)
encoding = result['encoding']

# Считываем CSV-файл
df = pd.read_csv('adresses.csv', encoding=encoding, engine='python', sep=';')
print(df.columns)

# Применяем функцию format_address к каждому адресу в столбце 'Address'
df['Address'] = df['Address'].apply(format_address)

# Сохраняем результат в новый CSV-файл

df.to_excel('formated.xlsx')
