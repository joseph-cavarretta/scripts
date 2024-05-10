bucket_name=test-bucket
date=20230106
gcloud sql export csv mysql-instance \
gs://$bucket_name/mysql_export/strava/$date/activities.csv \
--database=strava \
--offload \
--query='SELECT * FROM activities;'