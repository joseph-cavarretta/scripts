import pandas as pd
import pyodbc
from datetime import datetime

DRIVER = ''
SERVER = ''
DATABASE = ''
SQL_FILE = ''
OUT_FILE = ''


def main():
    with connect_to_db() as conn:
        df = sql_to_dataframe(SQL_FILE, conn)
        df.to_csv(OUT_FILE, index=False)
        print('Data saved to csv!')


def connect_to_db():
    conn = pyodbc.connect(
        f'DRIVER={DRIVER};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        'Trusted_Connection=yes;'
    )
    print(conn)
    return conn


def sql_to_dataframe(file_path, connection):
    with open(file_path) as fp:
        sql = fp.read()
        startTime = datetime.now()
        df = pd.read_sql(sql, connection)
        print(f'SQL query runtime: {datetime.now() - startTime}')
    return df


if __name__=='__main__':
    main()