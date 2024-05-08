from google.cloud import bigquery
from google.api_core.exceptions import NotFound

project_id = 'test-project'
client = bigquery.Client(project=project_id)
dataset_names = ['dataset_1','dataset_1']
location = 'US'


def create_bigquery_dataset(dataset_name):
    """Creates a bigquery dataset. Checks first if the dataset exists.
    Args:
        dataset_name: str
    """
    dataset_id = f'{client.project}.{dataset_name}'
    try:
        client.get_dataset(dataset_id)
        print(f'Dataset {dataset_id} already exists')
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)
        print(f'Created dataset {client.project}.{dataset.dataset_id}')


if __name__ == '__main__':
    for name in dataset_names:
        create_bigquery_dataset(name)
