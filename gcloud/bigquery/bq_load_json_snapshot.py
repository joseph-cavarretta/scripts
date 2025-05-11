from google.cloud import bigquery
from google.api_core.exceptions import NotFound


DATE = "2023-01-06"
PROJECT_ID = 'test-project'
DATASET = 'dataset'
TABLE = 'table'
SOURCE_BUCKET = 'bucket'

DEST_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"
GCS_URI = f'gs://{PROJECT_ID}/{SOURCE_BUCKET}/{DATE}/*.json'

SCHEMA = [
    bigquery.SchemaField('id', 'INTEGER'),
    bigquery.SchemaField('name', 'STRING'),
    bigquery.SchemaField('start_date_local', 'TIMESTAMP')
]


def table_exists(client, dest_id):
    try:
        client.get_table(dest_id)
        return True
    
    except NotFound:
        return False
    

def create_table(client):
    table = bigquery.Table(DEST_ID, schema=SCHEMA)
    request = client.create_table(table)
    print(f'Created table {DEST_ID}')

    
def load_gcs_to_bigquery_snapshot_data(client):
    """Appends multiple JSON files for a single date to table"""
    job_config = bigquery.LoadJobConfig(
        schema = SCHEMA,
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition = 'WRITE_APPEND'
    )

    load_job = client.load_table_from_uri(
        GCS_URI, DEST_ID, job_config=job_config
    )
    
    load_job.result()
    table = client.get_table(DEST_ID)

    print(f'Loaded {table.num_rows} rows to table {DEST_ID}')


if __name__ == '__main__':
    client = bigquery.Client()

    if not table_exists(client):
        create_table(client)
    else:
        load_gcs_to_bigquery_snapshot_data(client)