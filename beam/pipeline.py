"""
"""

from __future__ import absolute_import

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the pipeline."""

  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectRunner',
      # CHANGE 3/5: Your project ID is required in order to run your pipeline on
      # the Google Cloud Dataflow Service.
      '--project=aou-res-curation-test',
      # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
      # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
      '--job_name=your-wordcount-job',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:

    # Read all of the EHR inputs.
    ehr_inputs = {}
    for tbl in ['person', 'measurement', 'condition_occurrence']:
      ehr_inputs[tbl] = {}
      for site in ['nyc_five_person', 'pitt_five_person']:
        ehr_inputs[tbl][site] = (
            p | f"{site}_{tbl}" >> beam.io.Read(beam.io.BigQuerySource(
                query=f"SELECT * FROM `aou-res-curation-test.calbach_prototype.{site}_{tbl}`",
                use_standard_sql=True))
            )

    # Merge tables across all sites.
    combined_by_domain = {}
    for tbl, data_by_site in ehr_inputs.items():
      combined_by_domain[tbl] = data_by_site.values() | f"ehr merge for {tbl}" >> beam.Flatten()

    # TODO:
    #   1. Move data from person table elsewhere.
    #   1. Row-level table transform
    #   1. Retract participants by ID.
    #   1. Group-by-participant transforms, e.g. remove duplicate measurements, observations
    #   1. Generate derived tables, e.g. mapping

    for domain, data in combined_by_domain.items():
      data | f"output for {domain}" >> beam.io.WriteToText(f"{domain}-out.txt")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
