import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadAllFromText, WriteToBigQuery


def run(argv=None):
    # argument parser
    parser = argparse.ArgumentParser()
    
    # pipeline options, google_cloud_options
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True
    
    p = beam.Pipeline(options=pipeline_options)

    p1 = p | 'trigger from pubsub' >> beam.io.ReadFromPubSub(topic='projects/PROJECT_ID/topics/TOPIC_NAME_1') \
        | "convert msg to dict" >> beam.Map(lambda x: json.loads(x)) \
        | "extract filename" >> beam.Map(lambda x : 'gs://{}/{}'.format(x['bucket'], x['name'])) \
        | "read file" >> ReadAllFromText() \
        | 'split' >> beam.Map(lambda x: x.split(',')) \
        | 'format to dict' >> beam.Map(lambda x: {"id": x[0], "name": x[1]})

    # Write the messages
    output_rec = p1 | 'write to BigQuery' >> WriteToBigQuery(
                    'PROJECT_ID:DATASET_ID.TABLE_NAME',
                    schema='id:INTEGER, name:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()
