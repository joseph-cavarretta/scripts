# This script loads a snapshot csv file into a sqlite table
# This is an absolute load, so table is truncated and replaced

import os, sys
import sqlite3
import pandas as pd

DB_PATH = 'db_path.sqlite'
TABLE = 'table'
CSV_PATH = 'csv_path'


def main():
    truncate_table()
    df = get_input_data()
    insert_records(df)


class MissingInputFileException(Exception):
    """ Raised when input file is missing """
    print(f'No input file found at {CSV_PATH}')
    pass


def input_file_present():
     if not os.path.isfile(CSV_PATH):
          return False
     else: 
          return True
          

def run_sql(db, sql):
    """Runs a sql command"""
    with sqlite3.connect(db) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print(f'{cursor.rowcount} rows affected')


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

        
def get_input_data():
    if input_file_present():
        df = pd.read_csv(CSV_PATH)
        return df
    else: 
        raise MissingInputFileException()


def truncate_table():
    print('Truncating table')
    sql = f'DELETE from {TABLE};'
    run_sql(DB_PATH, TABLE, sql)


def insert_records(dataframe):
    rows = dataframe.to_records(index=False).tolist()
    print(f'Loading {len(rows)} rows into table')
    insert_values(DB_PATH, TABLE, rows)


if __name__ == '__main__':
    main()