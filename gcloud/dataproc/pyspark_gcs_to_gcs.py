from pyspark.sql import SparkSession

BUCKET_NAME = 'test-bucket'
MASTER_NODE_INSTANCE_NAME = 'test-dataproc-cluster-m'


def build_session():
  return SparkSession.builder \
  .appName('spark_gcs_to_bq') \
  .getOrCreate()


def get_context(session):
  sc = session.sparkContext
  sc.setLogLevel('WARN')
  return sc


def create_dataframe(context):
  columns = ['id', 'start_date_local', 'type']
  files_rdd = context.textFile(f'gs://{BUCKET_NAME}/files/*')
  filtered_rdd = files_rdd.map(lambda x: x.type in ['Run', 'Ride', 'BackcountrySki'])
  df = filtered_rdd.toDF(columns)
  return df


def create_view(df, session):
  df.createOrReplaceTempView('activity_count_df')
  query = """
    SELECT
      id,
      start_date_local,
      type
    FROM activities
    WHERE type IN ('Run','Ride','BackcountrySki')
  """
  activity_count_df = session.sql(query)
  return activity_count_df


def write_to_gcs(df):
  df.write.save(
    f'gs://{MASTER_NODE_INSTANCE_NAME}/activities.csv', 
    format='csv', 
    mode='overwrite'
  )


if __name__ == '__main__':
  spark = build_session()
  sc = get_context(spark)
  df = create_dataframe(sc)
  view = create_view(df, spark)
  write_to_gcs(view)