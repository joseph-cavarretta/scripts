#!/bin/bash

# start service
net start mysql;

# connect
mysql -u root -p;

# stop service
net stop mysql;