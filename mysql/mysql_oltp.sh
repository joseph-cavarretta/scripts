#!/bin/bash

# get dummy data to load
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/oltp/oltpdata.csv

# start mysql server
start_mysql

# connect to mysql CLI
mysql --host=127.0.0.1 --port=3306 --user=root --password=""

# run commands in mysql CLI to create database and create table
create database sales;
use sales;
create table sales_data (product_id int, customer_id int, price int, quantity int, timestamp timestamp) engine = InnoDB

# in our project we loaded data vis myphpadmin, but here is an alternative for CLI
load data local infile "oltpdata.csv" into table sales_data \
fields terminated by "," \
enclosed by '"' \
lines terminated by '\n' \
(product_id, customer_id, price, quantity, timestamp);

# create an index on timestamp field
create index timestamp on sales_data.timestamp;
show index from sales_data;

# create a backup of database
mysqldump -u root -p sales sales_data > sales_data.sql