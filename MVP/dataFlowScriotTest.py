import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions


# Variables
project_id = "dataproject-2-413010"
subscription_name = "sub_dp2"
topic_name= "topic_dp2"
bq_dataset = "BBDD"
bq_table = "test3"
bucket_name = "test-dp2"


def decode_message(msg):

    output = msg.decode('utf-8')

    logging.info("New PubSub Message: %s", output)

    return json.loads(output)

def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        # save_main_session=True
        project=project_id,
        runner="DataflowRunner",
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="us-central1"
    )) as p:
        (
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name}')
            | "decode msg" >> beam.Map(decode_message)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{project_id}:{bq_dataset}.{bq_table}", # Required Format: PROJECT_ID:DATASET.TABLE
                schema='vehicle_id:INTEGER,location:STRING,num_plazas:INTEGER', # Required Format: field:TYPE
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")
    
    # Run Process
    run()