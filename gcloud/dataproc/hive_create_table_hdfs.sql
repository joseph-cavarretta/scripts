CREATE EXTERNAL TABLE activities(
    id INTEGER,
    start_date_local DATE,
    type STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location 'data/files'
TBLPROPERTIES ("skip.header.line.count"="1")
;
