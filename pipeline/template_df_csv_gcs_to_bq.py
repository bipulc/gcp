import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider
import logging
import argparse
import re

# Read from GCS and write to BQ Table

# Args:
#     input file uri
#     output table id (dataset_id.table_id)

schema =   'jobid:STRING, scenarioid:INT64, migration_pl:FLOAT64, default_pl:FLOAT64'

class RunTimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--input_file_path",
            help="uri of the input file"
        )
        parser.add_value_provider_argument(
            "--bq_table_id",
            help="dataset_id.BQ table id"
        )

class convertToDict:

    def parse_method (self, element):
        value = re.split(",", element)

        row = dict(zip(('jobid','scenarioid','migration_pl','default_pl'), value))
        return row

def run():

    data_ingestion = convertToDict()

    pipeline_options = PipelineOptions(
        streaming=False, save_main_session=True, project='data-analytics-bk', job_name='template-csv-gcs-to-bq'
    )

    pipeline = beam.Pipeline(options=pipeline_options)
    user_options = pipeline_options.view_as(RunTimeOptions)

    p =  (pipeline | "Read From Input Datafile" >> beam.io.ReadFromText(user_options.input_file_path)
                   | "Convert to Dict" >> beam.Map(lambda r: data_ingestion.parse_method(r))
                   | "Write to BigQuery Table" >> beam.io.WriteToBigQuery(table=user_options.bq_table_id,
                                                    schema=schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
          )

    pipeline.run()
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
