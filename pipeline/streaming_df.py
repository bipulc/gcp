#! /usr/bin/env python
# Dataflow code to process messages from PubSub Topic
# Read from PubSub, Add Publsih Timestamp, Write to BigQuery Table
# Base code - https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/pubsub/streaming-analytics/PubSubToGCS.py

import logging
import argparse
import json
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions

schema = 'name:STRING, address:STRING, country_code:STRING, city:STRING, bank_iban:STRING, company:STRING, credit_card:INT64, credit_card_provider:STRING, credit_card_expires:STRING, date_record_created:DATETIME, employement:STRING, emp_id:STRING, name_prefix:STRING, phone_number:STRING'
count_schema = 'country_name:STRING, num_cc_card:INT64'

class printOnConsole(beam.DoFn):
    def process(self, element):
        logging.info('Element : %s', element)

class convertToDict(beam.DoFn):
    # Convert messages in PubSub to Python Dictionaries for inserting into BigQuery Table

    def process(self, element):
        elem_dict = json.loads(element)
        return [{
            'name': elem_dict['name'],
            'address': elem_dict['address'],
            'country_code': elem_dict['country_code'],
            'city': elem_dict['city'],
            'bank_iban': elem_dict['bank_iban'],
            'company': elem_dict['company'],
            'credit_card': elem_dict['credit_card'],
            'credit_card_provider': elem_dict['credit_card_provider'],
            'credit_card_expires': elem_dict['credit_card_expires'],
            'date_record_created': elem_dict['date_record_created'],
            'employement': elem_dict['employement'],
            'emp_id': elem_dict['emp_id'],
            'name_prefix': elem_dict['name_prefix'],
            'phone_number': elem_dict['phone_number']
        }]


class count_cc(beam.DoFn):
    # Count the occurrences of each country for CC row.

    def process(self, element):
        (country_code, ones) = element
        return [{
        'country_code':country_code,
        'num_cc_card':sum(ones)
        }]

# Format the counts into a PCollection of strings.
def format_result(cc_count):
    (country_code, count) = cc_count
    return '%s: %d' % (country_code, count)


def run(project_name, pubsub_topic, pipeline_args):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True, project=project_name, job_name='strplccprocessing'
    )

    '''
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read PubSub messages" >> beam.io.ReadFromPubSub(topic=pubsub_topic)
            | "Decode PubSub messages" >> beam.Map(lambda x: x.decode('utf-8'))
            | "Convert to Dictionary" >> beam.ParDo(convertToDict())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery('{0}:da_streaming_pipeline.cc_account_info'.format(project_name), schema=schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        )

    result = pipeline.run()
    result.wait_until_finish()
    '''

    pipeline = beam.Pipeline(options=pipeline_options)
    process_pubsub_message = (pipeline
                              | "Read PubSub messages" >> beam.io.ReadFromPubSub(topic=pubsub_topic)
                              | "Decode PubSub messages" >> beam.Map(lambda x: x.decode('utf-8'))
                              | "Convert to Dictionary" >> beam.ParDo(convertToDict())
                              )

    #write_raw_data_to_bq = (process_pubsub_message
    #                        | "Write Raw data to BigQuery" >> beam.io.WriteToBigQuery(
    #            '{0}:da_streaming_pipeline.cc_account_info'.format(project_name), schema=schema,
    #            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

    # Take process_pubsub_message PCollection and count number of credit card / per county within a window of 60 seconds and write to another table in BQ
    num_cc_per_county = (process_pubsub_message
                         | "Extract County as Key/Value" >> beam.Map(lambda x: (x["country_code"].encode('utf-8'), 1))
                         | "Log Output" >> beam.ParDo(printOnConsole()))
                         #| "Fixed 60 Seconds Window" >> beam.WindowInto(window.FixedWindows(60))
                         #| "Group By on country code" >> beam.GroupByKey()
                         #| LogElements())
                        # | "Write temp output to GCS" >> beam.io.WriteToText('gs://da_streaming_pipeline/tempout'))
                        # | "Count No of CC per Country Code" >> beam.ParDo(count_cc()))

                        # | "Write Count to BigQuery" >> beam.io.WriteToBigQuery('{0}:da_streaming_pipeline.cc_count_per_country'.format(project_name), schema=count_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND ))

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
        "--pubsub_topic",
        help="PubSub topic in format projects/<PROJECT_NAME/topics/<TOPIC_NAME> to read from."
    )

    '''
    parser.add_argument(
      "--window_size",
      type=float,
      default=1.0,
      help="Processing window size in minutes"
    )
    parser.add_argument(
      "--output_path",
      help="GCS Path of the output file including filename prefix"
    )
    '''
    known_args, pipeline_args = parser.parse_known_args()

run(known_args.project_name, known_args.pubsub_topic, pipeline_args)
