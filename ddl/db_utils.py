import sqlite3

import concurrent.futures

import requests
import json
import time


def populate_stacje(conn):

    data = fetch_data('https://api.gios.gov.pl/pjp-api/v1/rest/station/findAll?page=0&size=500')  # Requesting API
    records = [(
        rec['Identyfikator stacji'],
        rec['Nazwa stacji'],
        rec['Województwo'],
        rec['WGS84 φ N'],
        rec['WGS84 λ E']
    ) for rec in data['Lista stacji pomiarowych']]

    insert_sql = """INSERT INTO Stacje (id, nazwa_stacji, wojewodztwo, latitude, longitude) VALUES (?, ?, ?, ?, ?)"""
    cursor = conn.cursor()
    cursor.executemany(insert_sql, records)
    conn.commit()



def populate_stanowiska_pomiarowe(conn):
    cursor = conn.cursor()
    sql = "SELECT id from Stacje"
    cursor.execute(sql)

    for id_stacji in cursor.fetchall():  # Get all results (list of tuples)

        data = fetch_data(f"https://api.gios.gov.pl/pjp-api/v1/rest/station/sensors/{id_stacji[0]}?size=500&sort=Id")
        records = [(
            rec['Identyfikator stanowiska'],
            rec['Identyfikator stacji'],
            rec['Wskaźnik'],
            rec['Wskaźnik - wzór'],
            rec['Wskaźnik - kod'],
            rec['Id wskaźnika']
        ) for rec in data['Lista stanowisk pomiarowych dla podanej stacji']]

        insert_sql = """INSERT INTO Stanowiska_pomiarowe (id, id_stacji, wskaznik, wskaznik_wzor, wskaznik_kod, id_wskaznik) VALUES (?, ?, ?, ?, ?, ?)"""
        cursor = conn.cursor()
        cursor.executemany(insert_sql, records)
    conn.commit()


def populate_historja_pomiarow(conn):



    cursor = conn.cursor()
    sql = "SELECT id from Stanowiska_pomiarowe"
    cursor.execute(sql)

    for id_stanowiska in cursor.fetchall():

        data = fetch_data(f"https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/{id_stanowiska[0]}?size=500&sort=Data&dayNumber={150}&page={0}&size=500")
        number_of_pages = data['totalPages']

        result = []
        result.extend([(rec['Nazwa stacji'], rec['Kod stanowiska'], rec['Data'], rec['Wartość'])
                       for rec in data['Lista archiwalnych wyników pomiarów']])

#         with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
#             futures = []
#
#             for i in range(1, number_of_pages):
#                 future = executor.submit(fetch_data, f"https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/{id_stanowiska[0]}?size=500&sort=Data&dayNumber={150}&page={i}&size=500")
#                 futures.append(future)
#
#             # Process the results from the futures
#             for future in concurrent.futures.as_completed(futures):
#                 data = future.result()  # Wait for each future to complete
#                 if data:
#                     result.extend([(rec['Nazwa stacji'], rec['Kod stanowiska'], rec['Data'], rec['Wartość'])
#                                    for rec in data['Lista archiwalnych wyników pomiarów']])
#
        for i in range(1, number_of_pages):
            data = fetch_data(f"https://api.gios.gov.pl/pjp-api/v1/rest/archivalData/getDataBySensor/{id_stanowiska[0]}?size=500&sort=Data&dayNumber={150}&page={i}&size=500")
            result.extend([(rec['Nazwa stacji'], rec['Kod stanowiska'], rec['Data'], rec['Wartość'])
                               for rec in data['Lista archiwalnych wyników pomiarów']])

        insert_sql = """INSERT INTO Historja_pomiarow (Nazwa_stacji, Kod_stanowiska, date_time, wartosc) VALUES (?, ?, ?, ?)"""
        cursor = conn.cursor()
        cursor.executemany(insert_sql, result)

        print(f"Dane historyczne ze Stanowiska o Identyfikatorze: {id_stanowiska[0]} zostały załadowane")
        conn.commit()



def fetch_data(url):
    """Fetches data from the specified URL."""
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        time.sleep(1)
        return fetch_data(url)
    else:
        print(f"Error fetching data from {url}: {response.status_code}")
        return response.status_code

def create_table_from_ddl(conn, ddl_file):
    with open(ddl_file, 'r') as f:
        sql = f.read()
    try:
        conn.execute(sql)
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")


if __name__ == "__main__":

    conn = sqlite3.connect('mydatabase.db')

    create_table_from_ddl(conn, './ddl/stacje.sql')
    create_table_from_ddl(conn, './ddl/stanowiska_pomiarowe.sql')
    create_table_from_ddl(conn, './ddl/historja_pomiarow.sql')

    populate_stacje(conn)
    populate_stanowiska_pomiarowe(conn)
    populate_historja_pomiarow(conn)

    conn.commit()
    conn.close()
