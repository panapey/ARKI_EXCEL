import asyncio
import os
from datetime import datetime

import aiofiles
import mysql.connector
import openpyxl
import pandas as pd

db_config = {
    'host': 'HOST',
    'port': PORT,
    'user': 'USER',
    'password': 'PASSWORD',
    'db': 'DB',
}

current_date = datetime.now().strftime('%Y-%m-%d 06:00:00')

cnx = mysql.connector.connect(**db_config)

cursor = cnx.cursor()

path_directory = 'PATH_DIRECTORY'


async def file_reader():
    df = pd.read_excel('ARKI.xlsx', engine='openpyxl')

    group_names = df['group_name'].tolist()
    dota = {"Name": [], "Device ID": []}
    temp_podachi = {"Name": [], "Device ID": []}
    temp_obratki = {"Name": [], "Device ID": []}
    davl_podachi = {"Name": [], "Device ID": []}
    davl_obratki = {"Name": [], "Device ID": []}

    for filename in os.listdir(path_directory):
        if any(group_name in filename for group_name in group_names) and "КОТ" not in filename and "ВТЭ" in filename:
            async with aiofiles.open(os.path.join(path_directory, filename), mode='r') as file:
                async for line in file:
                    if "=" in line:
                        name, value = line.strip().split("=")
                        if "DevID" in name:
                            device_id = value
                        if "Теплоноситель" in value and "Гкал" in value:
                            dota["Device ID"].append(device_id)
                            dota["Name"].append(name)
                        if "Теплоноситель подача" in value and "°C" in value:
                            temp_podachi["Device ID"].append(device_id)
                            temp_podachi["Name"].append(name)
                        if "Теплоноситель обратка" in value and "°C" in value:
                            temp_obratki["Device ID"].append(device_id)
                            temp_obratki["Name"].append(name)
                        if "Теплоноситель подача" in value and "Bar" in value:
                            davl_podachi["Device ID"].append(device_id)
                            davl_podachi["Name"].append(name)
                        if "Теплоноситель обратка" in value and "Bar" in value:
                            davl_obratki["Device ID"].append(device_id)
                            davl_obratki["Name"].append(name)

    return dota, temp_podachi, temp_obratki, davl_podachi, davl_obratki


def query_and_search(dota, temp_podachi, temp_obratki, davl_podachi, davl_obratki):
    df = pd.read_excel('ARKI.xlsx', engine='openpyxl')

    group_ids = df['group_id'].tolist()

    device_ids = dota["Device ID"]
    names = dota["Name"]
    results_dota = []
    for group_id, device_id, name in zip(group_ids, device_ids, names):
        query_dota = f"""
                        SELECT dev.DEVICE_ID,g.ID_OWNER, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
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
                        ON dev.DEVICE_ID = g.DEVICE_ID
                        WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) 
                        and a.ID_DEVICE = {device_id}
                        and a.ADAPTER_NAME = "Часовой архив"
                        and ap.PARAMETER_NAME = "{name}"
                        ORDER BY dev.DEVICE_NAME;
                            """

        cursor.execute(query_dota)

        rows = cursor.fetchall()

        results_dota.extend(rows)

    device_ids = temp_podachi["Device ID"]
    names = temp_podachi["Name"]
    results_temp_podachi = []
    for group_id, device_id, name in zip(group_ids, device_ids, names):
        query_temp_podachi = f"""
                                SELECT dev.DEVICE_ID,g.ID_OWNER, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
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
                                ON dev.DEVICE_ID = g.DEVICE_ID
                                WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) 
                                and a.ID_DEVICE = {device_id}
                                and a.ADAPTER_NAME = "Часовой архив"
                                and ap.PARAMETER_NAME = "{name}"
                                ORDER BY dev.DEVICE_NAME;
                                    """

        cursor.execute(query_temp_podachi)

        rows = cursor.fetchall()

        results_temp_podachi.extend(rows)

    device_ids = temp_obratki["Device ID"]
    names = temp_obratki["Name"]
    results_temp_obratki = []
    for group_id, device_id, name in zip(group_ids, device_ids, names):
        query_temp_obratki = f"""
                                SELECT dev.DEVICE_ID,g.ID_OWNER, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
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
                                ON dev.DEVICE_ID = g.DEVICE_ID
                                WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) 
                                and a.ID_DEVICE = {device_id}
                                and a.ADAPTER_NAME = "Часовой архив"
                                and ap.PARAMETER_NAME = "{name}"
                                ORDER BY dev.DEVICE_NAME;
                                    """

        cursor.execute(query_temp_obratki)

        rows = cursor.fetchall()

        results_temp_obratki.extend(rows)

    device_ids = davl_podachi["Device ID"]
    names = davl_podachi["Name"]
    results_davl_podachi = []
    for group_id, device_id, name in zip(group_ids, device_ids, names):
        query_davl_podachi = f"""
                                    SELECT dev.DEVICE_ID,g.ID_OWNER, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
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
                                    ON dev.DEVICE_ID = g.DEVICE_ID
                                    WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) 
                                    and a.ID_DEVICE = {device_id}
                                    and a.ADAPTER_NAME = "Часовой архив"
                                    and ap.PARAMETER_NAME = "{name}"
                                    ORDER BY dev.DEVICE_NAME;
                                        """

        cursor.execute(query_davl_podachi)

        rows = cursor.fetchall()

        results_davl_podachi.extend(rows)

    device_ids = davl_obratki["Device ID"]
    names = davl_obratki["Name"]
    results_davl_obratki = []
    for group_id, device_id, name in zip(group_ids, device_ids, names):
        query_davl_obratki = f"""
                                    SELECT dev.DEVICE_ID,g.ID_OWNER, dev.DEVICE_NAME, ap.PARAMETER_NAME, d.MEASURE_VALUE
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
                                    ON dev.DEVICE_ID = g.DEVICE_ID
                                    WHERE (r.RECORD_TIME = '{current_date}' or r.RECORD_TIME is NULL) 
                                    and a.ID_DEVICE = {device_id}
                                    and a.ADAPTER_NAME = "Часовой архив"
                                    and ap.PARAMETER_NAME = "{name}"
                                    ORDER BY dev.DEVICE_NAME;
                                        """

        cursor.execute(query_davl_obratki)

        rows = cursor.fetchall()

        results_davl_obratki.extend(rows)

    df_results_dota = pd.DataFrame(results_dota, columns=['DEVICE_ID', 'GROUP_ID', 'ID_OWNER', 'DEVICE_NAME', 'VALUE'])
    df_results_temp_podachi = pd.DataFrame(results_temp_podachi,
                                           columns=['DEVICE_ID', 'GROUP_ID', 'ID_OWNER', 'DEVICE_NAME', 'VALUE'])
    df_results_temp_obratki = pd.DataFrame(results_temp_obratki,
                                           columns=['DEVICE_ID', 'GROUP_ID', 'ID_OWNER', 'DEVICE_NAME', 'VALUE'])
    df_results_davl_podachi = pd.DataFrame(results_davl_podachi,
                                           columns=['DEVICE_ID', 'GROUP_ID', 'ID_OWNER', 'DEVICE_NAME', 'VALUE'])
    df_results_davl_obratki = pd.DataFrame(results_davl_obratki,
                                           columns=['DEVICE_ID', 'GROUP_ID', 'ID_OWNER', 'DEVICE_NAME', 'VALUE'])

    book = openpyxl.load_workbook('111.xlsx')
    sheet = book['РППУ']

    df_excel = pd.read_excel('111.xlsx', sheet_name='РППУ')
    df_excel['G'] = df_excel['G'].astype(str)
    df_results_dota['GROUP_ID'] = df_results_dota['GROUP_ID'].astype(str)
    df_results_temp_podachi['GROUP_ID'] = df_results_temp_podachi['GROUP_ID'].astype(str)
    df_results_temp_obratki['GROUP_ID'] = df_results_temp_obratki['GROUP_ID'].astype(str)
    df_results_davl_podachi['GROUP_ID'] = df_results_davl_podachi['GROUP_ID'].astype(str)
    df_results_davl_obratki['GROUP_ID'] = df_results_davl_obratki['GROUP_ID'].astype(str)

    df_results_dota = df_results_dota[df_results_dota['GROUP_ID'].apply(
        lambda x: df_excel['G'].str.contains(x).any() if isinstance(x, str) else False)]
    df_results_temp_podachi = df_results_temp_podachi[df_results_temp_podachi['GROUP_ID'].apply(
        lambda x: df_excel['G'].str.contains(x).any() if isinstance(x, str) else False)]
    df_results_temp_obratki = df_results_temp_obratki[df_results_temp_obratki['GROUP_ID'].apply(
        lambda x: df_excel['G'].str.contains(x).any() if isinstance(x, str) else False)]
    df_results_davl_podachi = df_results_davl_podachi[df_results_davl_podachi['GROUP_ID'].apply(
        lambda x: df_excel['G'].str.contains(x).any() if isinstance(x, str) else False)]
    df_results_davl_obratki = df_results_davl_obratki[df_results_davl_obratki['GROUP_ID'].apply(
        lambda x: df_excel['G'].str.contains(x).any() if isinstance(x, str) else False)]

    print(df_results_dota)
    if not df_results_dota.empty:
        for index, value in enumerate(df_results_dota['VALUE'], start=7):
            sheet[f'M{index}'] = value

    else:
        print("No data to write to Excel.")

    if not df_results_temp_podachi.empty:
        for index, value in enumerate(df_results_temp_podachi['VALUE'], start=7):
            sheet[f'I{index}'] = value

    else:
        print("No data to write to Excel.")

    if not df_results_temp_obratki.empty:
        for index, value in enumerate(df_results_temp_obratki['VALUE'], start=7):
            sheet[f'J{index}'] = value

    else:
        print("No data to write to Excel.")

    if not df_results_davl_podachi.empty:
        for index, value in enumerate(df_results_davl_podachi['VALUE'], start=7):
            sheet[f'K{index}'] = value

    else:
        print("No data to write to Excel.")

    if not df_results_davl_obratki.empty:
        for index, value in enumerate(df_results_davl_obratki['VALUE'], start=7):
            sheet[f'L{index}'] = value

    else:
        print("No data to write to Excel.")

    book.save('111.xlsx')
    book.close()

    cnx.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    dota, temp_podachi, temp_obratki, davl_podachi, davl_obratki = loop.run_until_complete(file_reader())
    print(len(dota["Device ID"]))
    query_and_search(dota, temp_podachi, temp_obratki, davl_podachi, davl_obratki)
