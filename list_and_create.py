import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadAllFromText, WriteToText


def run(argv=None):
    # argument parser
    parser = argparse.ArgumentParser()
    
    # pipeline options, google_cloud_options
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True

    file_list = ['gs://bucket_1/folder_1/file.csv', 'gs://bucket_2/data.csv']
    
    p = beam.Pipeline(options=pipeline_options)
    
    p1 = p | "create PCol from list" >> beam.Create(file_list) \
        | "read files" >> ReadAllFromText() \
        | "transform" >> beam.Map(lambda x: x) \
        | "write to GCS" >> WriteToText('gs://bucket_3/output')

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()