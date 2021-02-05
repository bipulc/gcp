#! /usr/bin/env python3
# Use case - Pipeline to insert data into multiple BigQuery Tables
# An OLTP application need to push data to BigQuery for Analytics purposes. The application has approximately 100 tables.
# Data can be pushed in real time via PubSub or in Batch via GCS
# This script will use Dynamic Destination feature of beam.io.WriteToBigQuery to determine destination table and tableschema based on TableRow data

# Import relevant packages

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import argparse
import json

# TODO: Setup runtime options for template

# Read the Source data and convert into Dictionary

class parseInputData:

    def getData (self, element):
        row = json.loads(element.decode('utf-8'))
        return row

def loadSchema(element):
    row = json.loads(element)
    return (row["tablename"], row["schema"])

# Main Function

def run (project_name, data_topic, schema_def_file, pipeline_args):

    data_ingestion = parseInputData()

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True, project=project_name, job_name='multi-table-stream-pl-dyndest'
    )


    with beam.Pipeline(options=pipeline_options) as pipeline:
        #
        # Create a PCollection of table_name and schema  from schema def file on GCS  .
        # It will be passed as as Side Input to WriteToBigQuery
        #

        schema_coll = beam.pvalue.AsDict( pipeline
                        | "Read from  Schema Defn File" >> beam.io.ReadFromText(schema_def_file)
                        | "Get Schema Defn" >> beam.Map(lambda x : loadSchema(x))
                   )


        # Main Processing - Read input from a PubSub Topic.
        # The row contains name of the table where this row will be ingested. The tablename will also be inserted as one of the column.
        # TODO: Need a better way to identify target table as in this implementation the tablename is unnecessarily insert into the table
        # Schema is derived from Side Input PCollection.
        # BigQuery table will be created if needed. Rows will be appended

        (
            pipeline | "Read Data From Input Topic" >> beam.io.ReadFromPubSub(topic=data_topic)
                     | "Get Table data from input row" >> beam.Map(lambda r : data_ingestion.getData(r))
                     | "Write to BigQuery Table" >> beam.io.WriteToBigQuery(table = lambda row: row['tablename'],
                                                                            schema = lambda table, schema_coll : schema_coll[table],
                                                                            schema_side_inputs=(schema_coll,),
                                                                            create_disposition='CREATE_IF_NEEDED',
                                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
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
      "--data_topic",
      help="PubSub topic in format projects/<PROJECT_NAME/topics/<TOPIC_NAME> for data"
    )

    parser.add_argument(
      "--schema_def_file",
      help="uri of the schema def file on GCS"
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.project_name, known_args.data_topic, known_args.schema_def_file, pipeline_args )