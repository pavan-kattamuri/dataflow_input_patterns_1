import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadAllFromText, WriteToText, WriteToBigQuery


def run(argv=None):
    # argument parser
    parser = argparse.ArgumentParser()
    
    # pipeline options, google_cloud_options
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True
    
    p = beam.Pipeline(options=pipeline_options)
    
    # Create two PCollections by reading from two different pubsub topics
    p1 = p | 'read from topic 1' >> beam.io.ReadFromPubSub(topic='projects/PROJECT_ID/topics/TOPIC_NAME_1')
    p2 = p | 'read from topic 2' >> beam.io.ReadFromPubSub(topic='projects/PROJECT_ID/topics/TOPIC_NAME_2')

    # Merge the two PCollections
    merged = (p1, p2) | 'merge sources' >> beam.Flatten()
    # Convert to dict
    rec_dict = merged | 'convert to dict' >> beam.Map(lambda x: json.loads(x))

    # Write the messages
    rec_dict | 'write to GCS' >> WriteToBigQuery(
                    'PROJECT_ID:DATASET_ID.TABLE_NAME',
                    schema='id:INTEGER, name:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    run()
