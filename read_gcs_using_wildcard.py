import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadFromText, WriteToText


def run(argv=None):
    # argument parser
    parser = argparse.ArgumentParser()
    
    # pipeline options, google_cloud_options
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True

    p = beam.Pipeline(options=pipeline_options)
    
    rec = p | "read from GCS" >> ReadFromText("gs://bucket_name/folder_path/file*.csv") \
        | "transform" >> beam.Map(lambda x: x) \
        | "write to GCS" >> WriteToText(known_args.output_path)

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()
