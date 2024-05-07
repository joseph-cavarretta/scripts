import os
import configparser
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

CONFIG_PATH = ''


class MissingConfigException(Exception):
    """ Raised when config file is missing """
    pass


def _read_config() -> configparser.ConfigParser:
    """ Reads config file"""
    try:
        config_parser = configparser.ConfigParser()
        return config_parser.read(CONFIG_PATH)
    
    except Exception as e: # get specific expection
        raise MissingConfigException(e)


def mysql_connect():
    """ Returns mysql connection object """
    config_parser = _read_config()
    return mysql.connector.connect(
        user=config_parser.get('MYSQL', 'username'),
        password=config_parser.get('MYSQL', 'password'),
        host=config_parser.get('MYSQL', 'hostname'),
        database=config_parser.get('MYSQL', 'db')
    )


def mysql_get_creds():
    """ Returns dict of connection creds """
    config_parser = _read_config()
    return dict(
        user=config_parser.get('MYSQL', 'username'),
        password=config_parser.get('MYSQL', 'password'),
        host=config_parser.get('MYSQL', 'hostname'),
        database=config_parser.get('MYSQL', 'db'),
        port=config_parser.get('MYSQL', 'port')
    )


def mysql_to_df(query: str) -> pd.DataFrame:
    """ Returns pandas dataframe from query """
    with mysql_connect() as conn:
        df = pd.read_sql(query, con=conn)
    return df


def mysql_query(query: str) -> list:
    """ Returns list of records from query """
    with mysql_connect() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        records = [row for row in cursor.fetchall()]
    return records


def mysql_insert_row(query: str, data: tuple) -> None:
    """ Inserts a single row """
    with mysql_connect() as conn:
        cursor = conn.cursor()
        cursor.execute(query, data)
        conn.commit()
        print(f'{cursor.rowcount} rows inserted')


def mysql_insert_multiple(query: str, data: list) -> None:
    """ Inserts multiple rows """
    with mysql_connect() as conn:
        cursor = conn.cursor()
        cursor.executemany(query, data)
        conn.commit()
        print(f'{cursor.rowcount} rows inserted')


def mysql_insert_dataframe(dataframe: pd.DataFrame, table: str) -> None:
    """ Inserts (appends) a pandas DataFrame """
    creds = mysql_get_creds()
    conn_string = f'mysql+mysqlconnector://{creds["user"]}:{creds["password"]}\
        @{creds["host"]}:{creds["port"]}/{creds["db"]}'
    eng = create_engine(conn_string, echo=False)
    dataframe.to_sql(name=table, con=eng, if_exists='append', index=False)
    print(f'{len(dataframe)} rows inserted')
