# from dataproc master node shell
gsutil cp gs://test-bucket/*.csv ./

# any data in the master node can be loaded into hdfs like this:
hdfs dfs -mkdir ../../data/
hdfs dfs -mkdir ../../data/files
hdfs dfs -put raw_data.csv ../../data/files

# list files
hdfs dfs -ls ../../data/strava_files