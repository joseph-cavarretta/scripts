#!/bin/sh

# setup staging database with mysql
start_mysql
mysql --host=127.0.0.1 --port=3306 --user=root --password=mypassword

# run the following in mysql shell

# create database
create database sales

# run following in terminal

# get dummy schema and data
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/sales.sql

# import database schema and data
mysql -u root -p sales < sales.sql

# get additional dummy data to load
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/sales.csv

# run automation.py to pull new records from staging (mysql) 
# and load into production data warehouse (db2)
python3 automation.py

# create airflow DAG and schedule to run job
start airflow

# get dummy data to transform
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/accesslog.txt
# see process_web_log.py to see DAG and pipeline steps for processing this file

# submit airflow dag
cp process_web_log.py $AIRFLOW_HOME/dags
airflow dags list | grep process_web_log
airflow dags unpause process_web_log
