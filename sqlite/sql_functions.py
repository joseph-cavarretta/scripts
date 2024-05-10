import sqlite3
import pandas as pd


def sql_query(db, sql):
    """Returns a dataframe of results"""
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        query = cursor.execute(sql)
        cols = [column[0] for column in query.description] # not optimized for when you only need values in list
        df = pd.DataFrame.from_records(data=query.fetchall(), columns=cols)
        return df
    

def insert_values(db, table, rows):
    """Inserts rows (list of tuples) into database"""
    vals_len = len(rows[0])
    vals_string = ('?,' * (vals_len - 1)) + '?'
    sql = f'INSERT INTO {table} VALUES ({vals_string})'
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        for row in rows:
            print(row)
            cursor.executemany(sql, row)
        conn.commit()
        print(f'{cursor.rowcount} rows inserted')
        


def run_sql(db, sql):
    """Runs a sql command"""
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print(f'{cursor.rowcount} rows affected')