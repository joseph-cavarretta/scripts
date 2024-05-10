python3 beam_test.py \
--project=$PROJECT_ID \
--region=$REGION \
--runner=DirectRunner \ # runs locally for testing purposes
# --runner=DataflowRunner # runs on Dataflow in production
--temp_location=gs://$BUCKET_NAME/processed_activities