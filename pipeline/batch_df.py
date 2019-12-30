#! /usr/bin/env python
# Dataflow code to process messages from BigQuery Table
# Read from BigQuery, Apply an aggregation and write output back to another BigQuery Table
# Base code - https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/pubsub/streaming-analytics/PubSubToGCS.py
"""
Command Line Execution of this pipeline

python -m batch_df \
  --project_name data-analytics-bk \
  --dataset_name da_streaming_pipeline \
  --input_data_table cc_account_info \
  --output_data_table cc_count_per_country \
  --runner Dataflow \
  --staging_location gs://da_batch_pipeline/stage \
  --temp_location gs://da_batch_pipeline/temp \
  --region europe-west1 --zone=europe-west2-b \
  --dataflow_kms_key projects/data-analytics-bk/locations/global/keyRings/str-pl-global-kr/cryptoKeys/str-pl-global-key01
"""


import logging
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

count_schema = 'country_name:STRING, num_cc_card:INT64'


class printOnConsole(beam.DoFn):
    def process(self, element):
        logging.info('Element : %s', element)


class count_cc(beam.DoFn):
    # Count the occurrences of each country for CC row.

    def process(self, element):
        (country_code, ones) = element
        return [{
            'country_name': country_code,
            'num_cc_card': sum(ones)
        }]


def run(project_name, dataset_name, input_data_table, output_data_table, pipeline_args):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=False, save_main_session=True, project=project_name, job_name='batchplccprocessing'
    )

    pipeline = beam.Pipeline(options=pipeline_options)
    count_cc_per_country = (pipeline
                            | "Read From BigQuery Table " >> beam.io.Read(
                beam.io.BigQuerySource('{0}:{1}.{2}'.format(project_name, dataset_name, input_data_table)))
                            | "Extract Country Code column" >> beam.Map(
                lambda elem: (elem["country_code"].encode('utf-8'), 1))
                            | "Group by Country Code" >> beam.GroupByKey()
                            | "Count Num CC Per Country" >> beam.ParDo(count_cc())
                            | "Write Count to BigQuery Output Table" >> beam.io.WriteToBigQuery(
                '{0}:{1}.{2}'.format(project_name, dataset_name, output_data_table), schema=count_schema,
                                     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                            )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_name",
        help="name of GCP project"
    )
    parser.add_argument(
        "--dataset_name",
        help="name of table dataset"
    )
    parser.add_argument(
        "--input_data_table",
        help="BigQuery Table to read input data from."
    )
    parser.add_argument(
        "--output_data_table",
        help="BigQuery Table to write output data to"
    )

    known_args, pipeline_args = parser.parse_known_args()

run(known_args.project_name, known_args.dataset_name, known_args.input_data_table, known_args.output_data_table,
    pipeline_args)
