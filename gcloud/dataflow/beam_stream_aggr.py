import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

INPUT_SUBSCRIPTION= 'projects/test-data/subscriptions/activity-sub-1'
OUTPUT_TABLE = 'test-project:strava.activities_stream'

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
beam_options = PipelineOptions(beam_args, streaming=True)

class BuildRecordFn(beam.DoFn):
    def process(self, element,  window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().isoformat()
        return [element + (window_start,)]

def run():
    with beam.Pipeline(options=beam_options) as p:(
        p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | "Parse JSON" >> beam.Map(json.loads)
        | "UseFixedWindow" >> beam.WindowInto(beam.window.FixedWindows(60))
        | 'Group By Activity' >> beam.Map(lambda elem: (elem['id'], elem['duration_sec']))
        | 'Sum' >> beam.CombinePerKey(sum)
        | 'AddWindowEndTimestamp' >> (beam.ParDo(BuildRecordFn()))
        #| 'Print' >> beam.Map(print)
        | 'Parse to JSON' >> beam.Map(lambda x : {'id': x[0],'sum_duration_sec':x[1],'window_timestamp':x[2]})
        | 'Write to Table' >> beam.io.WriteToBigQuery(OUTPUT_TABLE,
                        schema='id:STRING,sum_duration_sec:INTEGER,window_timestamp:TIMESTAMP',
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()