import csv
import luigi
import os
import math
import psycopg2
import pandas as pd
import re
import pyodbc


# No funciona en mac
class NeptunoDB:
    def connection():
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=input/Neptuno.mdb;')
        cursor = conn.cursor()
        cursor.execute('select * from table categorias')

        for row in cursor.fetchall():
            print (row)

class SupportSystemDecisionsDB:
    def connection():
        host = os.getenv('PG_UADE_BI_HOST', 'localhost')
        database = os.getenv('PG_UADE_BI_DATABASE', 'support_system_decisions_staging')
        user = os.getenv('PG_UADE_BI_USER', 'postgres')
        password = os.getenv('PG_UADE_BI_PASSWORD', '')
        port = os.getenv('PG_UADE_BI_PORT', 5532)

        return psycopg2.connect(
            dbname = database,
            user = user,
            host = host,
            password = password,
            port = port
        )

class DBRead:
    def get(table):
        connection = SupportSystemDecisionsDB.connection()
        try:
            cursor = connection.cursor()
            query = 'SELECT * FROM {}'.format(table)
            cursor.execute(query)
            return cursor.fetchall()

        finally:
            if(connection):
                cursor.close()
                connection.close()

class TransforHelper:
    def date_to_id(iso):
        if ((iso != '') or (iso)) and (type(iso) is not float):
            split = iso[0:10].split('-')
            return '{}{}{}'.format(split[2], split[1], split[0])
        else:
            return None

    def text_clean(text):
        if text is None:
            text = ''
        return re.sub(r'[^\w\s_|\.]+', '', text).strip()

    def remove_currency(price):
        if price is None:
            price = ''
        return price.split(' ')[0]

    def get_currency(price):
        if price is None:
            return 'EUR'
        symbol = price.split(' ')[1]
        dic = { '€': 'EUR', '$': 'USD'}

        if symbol in dic:
            return dic[symbol]
        else:
            return 'EUR'


class SliceableDict(dict):
    def slice(self, *keys):
        return {k: self[k] for k in keys}


class PgInsert:
    def exec(table, columns, csv_path):
        df = pd.read_csv(csv_path)
        df.columns = columns
        insert_sql = '''
                     INSERT INTO {0}
                        ( {1} ) VALUES ( {2} )
                     ON CONFLICT (id)
                        DO UPDATE SET
                        ( {1} ) = ( {2} )
                    '''.format(table, ','.join(columns), ','.join(['%s'] * len(columns)))

        connection = SupportSystemDecisionsDB.connection()
        try:
            cursor = connection.cursor()
            for row in df.values.tolist():
                # row + row is for the upsert that duplicate the value list
                #
                # INSERT INTO dim_categorias
                #     ( id,name) VALUES ( %s,%s )
                #  ON CONFLICT (id)
                #     DO UPDATE SET
                #     ( id,name) = ( %s,%s )
                cursor.execute(insert_sql, row + row)


            return connection.commit()

        finally:
            if(connection):
                cursor.close()
                connection.close()
