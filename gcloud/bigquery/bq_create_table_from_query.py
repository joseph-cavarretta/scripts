from google.cloud import bigquery

source_project_id = 'test-project'
source_dataset = 'test-dataset-stg'
source_table = 'test-table'

dest_project_id = 'test-project'
dest_dataset = 'test-dataset-latest'
dest_table = 'test-table'

COLUMNS = ['col1', 'col2']
SOURCE_ID = f'{source_project_id}.{source_dataset}.{source_table}'
DEST_ID = f'{dest_project_id}.{dest_dataset}.{dest_table}'


def create_table():
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination=DEST_ID,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    cols_str = ','.join(COLUMNS)
    sql = f'SELECT {cols_str} FROM `{SOURCE_ID}`;'

    job = client.query(sql, job_config=job_config)
    
    try:
        job.result()
        print('Table load success')
    except Exception as e:
            print(e)


if __name__ == '__main__':
    create_table()
