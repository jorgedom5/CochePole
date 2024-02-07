import pandas as pd
import os
from google.cloud import pubsub_v1
import threading
import argparse
import logging
import random
import string
import json
import time
from google.cloud import bigquery

# Input arguments
parser = argparse.ArgumentParser(description=('User Data Generator'))

parser.add_argument(
    '--project_id',
    required=True,
    help='GCP cloud project name.')
parser.add_argument(
    '--topic_name',
    required=True,
    help='PubSub topic name.')

args, opts = parser.parse_known_args()

def viaje_cliente():
    try:
        client = bigquery.Client()

        dataset_id = 'BBDD'
        table_id = 'tabla_viajes_1'

        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        query = f"""
                SELECT
                    latitud,
                    longitud,
                FROM
                    `{dataset_id}.{table_id}`
                ORDER BY
                    RAND()
                LIMIT 1
            """
        df_users = client.query(query).to_dataframe()

        df_users['usuario_id'] = random.randint(1, 100000)
        return df_users

    except Exception as e:
        logging.error(f"Error al obtener viaje aleatorio: {str(e)}")

class PubSubMessages:
    def __init__(self, project_id: str, topic_name: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message: str):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("New user needs a ride. Id: %s", message['usuario_id'])
        print(f"Published message: {json_str}")

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
    
    def insert_into_pubsub(self, pubsub_class, df_users):
        for index, row in df_users.iterrows():
            user_payload = {
                "usuario_id": int(row["usuario_id"]),
                "latitud": float(row["latitud"]),
                "longitud": float(row["longitud"]),
            }
            pubsub_class.publishMessages(user_payload)
            time.sleep(1)

def main():
    pubsub_class = PubSubMessages(args.project_id, args.topic_name)
    try:
        while True:
            df_users = viaje_cliente()
            pubsub_class.insert_into_pubsub(pubsub_class, df_users)
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Script interrumpido por el usuario.")
    finally:
        pubsub_class.__exit__()
        logging.info("Cerrando el script de forma ordenada (si es necesario).")


if __name__ == "__main__":
    main()
