CREATE TABLE test_table (
    id INT64,
    start_date_local DATE,
    type STRING
)
PARTITION BY (start_date_local);