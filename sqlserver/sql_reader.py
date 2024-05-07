"""
Created on Mon Mar 8 16:42:31 2021
@author: joe.cavarretta
"""
import pandas as pd
import pyodbc
from datetime import datetime

def main():
    conn = connect_to_db()
    df = readSQL('sql_file_name_here.sql', conn)
    df.to_csv('file_name_here.csv', index = False)
    print('Data saved to CSV!')


def connect_to_db():
    conn = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=;'
        'DATABASE=;'
        'Trusted_Connection=yes;'
    )
    print(conn)
    return conn


def readSQL(file_path, connection):
    with open(file_path) as fp:
        sql = fp.read()
        startTime = datetime.now()
        df = pd.read_sql(sql, connection)
        print(f'SQL query runtime: {datetime.now() - startTime}')
    return df


if __name__=='__main__':
    main()