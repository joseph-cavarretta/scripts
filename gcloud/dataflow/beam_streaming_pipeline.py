import apache_beam as beam
import argparse
import json
import logging

from apache_beam.options.pipeline_options import PipelineOptions

INPUT_SUBSCRIPTION= 'projects/joe-test/strava-sub'
OUTPUT_TABLE = 'joe-test-project:strava.raw_activities'

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
beam_options = PipelineOptions(beam_args, streaming=True)

def run():
    with beam.Pipeline(options=beam_options) as p:(
        p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | "Parse JSON" >> beam.Map(json.loads)
        | 'Write to Table' >> beam.io.WriteToBigQuery(OUTPUT_TABLE,
                        schema='id:INTEGER,start_date_local:TIMESTAMP,type:STRING',
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()