import apache_beam as beam
import argparse
import logging

from apache_beam.transforms.combiners import Sample
from apache_beam.options.pipeline_options import PipelineOptions


INPUT_FILE = 'gs://joe-test-bucket/raw_files/strava_activites.csv'
OUTPUT_PATH = 'gs://joe-test-bucket/processed_files/processed_activities.csv'

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
beam_options = PipelineOptions(beam_args)

class Split(beam.DoFn):
    def process(self, element):
        rows = element.split(",")
        return [{
            'id': str(rows[0]),
            'start_date_local': str(rows[1]),
            'type': str(rows[2])
        }]

def split_map(records):
    rows = records.split(",")
    return {
        'id': str(rows[0]),
        'start_date_local': str(rows[1]),
        'type': str(rows[2])
    }

def run():
    with beam.Pipeline(options=beam_options) as p:(
        p
        | 'Read' >> beam.io.textio.ReadFromText(INPUT_FILE)
        #| 'Split' >> beam.Map(split_map)
        | 'Split' >> beam.ParDo(Split())
        | 'Get Type' >> beam.Map(lambda s: (s['type'], 1))
        | 'Count per Key' >> beam.combiners.Count.PerKey()
        | 'Sample' >> Sample.FixedSizeGlobally(10)
        #| 'Print' >> beam.Map(print)
        | 'Write' >> beam.io.textio.WriteToText(OUTPUT_PATH)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
