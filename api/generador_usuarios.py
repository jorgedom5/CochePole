import pandas as pd
from google.cloud import pubsub_v1
import argparse
import logging
import random
import json
import time
from google.cloud import bigquery
from faker import Faker
from concurrent.futures import ThreadPoolExecutor

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

fake = Faker()

class PubSubMessages:
    def __init__(self, project_id: str, topic_name: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message: str):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("New user needs a ride. Id: %s", message['cliente_id'])
        print(f"Published message: {json_str}")

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
    
    def insert_into_pubsub(self, pubsub_class, df_users):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(pubsub_class.publishMessages, {
                "cliente_id": row["cliente_id"],
                "viaje_id": row["viaje_id"],
                "latitud": float(row["latitud"]),
                "longitud": float(row["longitud"]),
            }) for _, row in df_users.iterrows()]

            for future in futures:
                future.result()

def viaje_cliente(prev_users):
    try:
        client = bigquery.Client()

        dataset_id = 'BBDD'
        table_id = 'tabla_viajes_1'

        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        query = f"""
                SELECT
                    viaje_id,
                    latitud,
                    longitud,
                FROM
                    `{dataset_id}.{table_id}`
                ORDER BY
                    RAND()
                LIMIT 5
                """
        df_users = client.query(query).to_dataframe()

        # Agregar nuevos usuarios y viajes a los anteriores
        df_users["cliente_id"] = [fake.unique.random_int(1, 1000000) for _ in range(len(df_users))]
        df_users = pd.concat([prev_users, df_users], ignore_index=True)

        return df_users

    except Exception as e:
        logging.error(f"Error al obtener viaje aleatorio: {str(e)}")

def main():
    pubsub_class = PubSubMessages(args.project_id, args.topic_name)
    prev_users = pd.DataFrame(columns=["cliente_id", "viaje_id", "latitud", "longitud"])

    try:
        while True:
            df_users = viaje_cliente(prev_users)
            pubsub_class.insert_into_pubsub(pubsub_class, df_users)
            prev_users = df_users  # Actualizar usuarios anteriores
            time.sleep(0.01)  
    except KeyboardInterrupt:
        logging.info("Script interrumpido por el usuario.")
    finally:
        pubsub_class.__exit__()
        logging.info("Cerrando el script de forma ordenada (si es necesario).")

if __name__ == "__main__":
    main()
    
