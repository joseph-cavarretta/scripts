import pandas as pd
from mysql_functions import *

# insert value
table = ''
sql = f"INSERT INTO {table} (col1, col2) VALUES (%s, %s)"
vals = [
    ('val1', 'val2')
]
mysql_insert_row(sql, vals)

# insert many
table = ''
sql = f"INSERT INTO {table} (col1, col2) VALUES (%s, %s)"
vals = [
    ('val1', 'val2'),
    ('val1', 'val2'),
    ('val1', 'val2')
]
mysql_insert_multiple(sql, vals)