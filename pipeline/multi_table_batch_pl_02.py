#! /usr/bin/env python3
# Use case - Pipeline to insert data into multiple BigQuery Tables
# An OLTP application need to push data to BigQuery for Analytics purposes. The application has approximately 100 tables.
# Data can be pushed in real time via PubSub or in Batch via GCS
#  This script is implementing one file / topic / subscription per table from source system for batch mode ingestion.
#  The pipeline will be one to one with source and sink. The code will iterate over all topics/files
#  and create a pipleine per pair of source/sink.

# Import relevant packages

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import argparse
import os
from google.cloud import storage
import json

# TODO: Setup runtime options for template

# Read Configuration file and return as dictionary

def readconfigfile(config_file_name):

    config_file_d = os.path.normpath(config_file_name).split(os.sep)
    config_bucket_id = config_file_d[1]
    config_file_name = config_file_d[2] + '/' + config_file_d[3]

    # Create Storage Client
    storage_client = storage.Client()

    # Read config file from GCS bucket and convert to Dict.
    bucket = storage_client.bucket(config_bucket_id)
    filedata = bucket.blob(config_file_name)
    json_data = filedata.download_as_string()
    return json.loads(json_data)

# Load schema into a string from file


def getschema(schemauri):

    schema_file_d = os.path.normpath(schemauri).split(os.sep)
    schema_bucket_id = schema_file_d[1]
    schema_file_name = schema_file_d[2] + '/' + schema_file_d[3]

    # Create Storage Client
    storage_client = storage.Client()

    # Read config file from GCS bucket and convert to Dict.
    bucket = storage_client.bucket(schema_bucket_id)
    filedata = bucket.blob(schema_file_name)
    json_data = filedata.download_as_string()
    return json_data.decode('utf-8')

# Read the Source data and convert into Dictionary

class convertToDict:

# Data will be in Newline delimited JSON format, Convert it to Dict and return

    def parse_method (self, element):
        row = json.loads(element)
        return row

# Main Function


def run (project_name, config_file_name, pipeline_args):

    data_ingestion = convertToDict()

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=False, project=project_name, job_name='multi-table-batch-pl'
    )

    # Read configuration from a file on GCS. Configuration data will contain Sourcefile Name, Target table name, Target Table Schema in JSON format
    cdata = readconfigfile(config_file_name)


    # Iterate for all source files, tables in the configuration.
    with beam.Pipeline(options=pipeline_options) as pipeline:
        for p in cdata['tablelist']:
            i_file_path = p['sourcefile']
            schemauri = p['schemauri']
            schema=getschema(schemauri)
            dest_table_id = p['targettable']

            (   pipeline | "Read From Input Datafile" + dest_table_id >> beam.io.ReadFromText(i_file_path)
                         | "Convert to Dict" + dest_table_id >> beam.Map(lambda r: data_ingestion.parse_method(r))
                         | "Write to BigQuery Table" + dest_table_id >> beam.io.WriteToBigQuery('{0}:{1}'.format(project_name, dest_table_id),
                                                                            schema=schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
            )



# Main function
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_name",
        help="name of GCP project"
    )

    parser.add_argument(
      "--config_file_name",
      help="uri of the config file on GCS"
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.project_name, known_args.config_file_name, pipeline_args )