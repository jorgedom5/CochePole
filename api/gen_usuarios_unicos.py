import pandas as pd
from google.cloud import pubsub_v1
import argparse
import logging
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
        logging.info("Updating user position. Id: %s", message['cliente_id'])
        print(f"Published message: {json_str}")

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
    
    def insert_into_pubsub(self, pubsub_class, df_users):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(pubsub_class.publishMessages, {
                "cliente_id": row["cliente_id"],
                "viaje_id": int(row["viaje_id"]),
                "latitud": float(row["latitud"]) + fake.random.uniform(-0.001, 0.001),
                "longitud": float(row["longitud"]) + fake.random.uniform(-0.001, 0.001),
            }) for _, row in df_users.iterrows()]

            for future in futures:
                future.result()

def obtener_datos_iniciales(cliente_ids, trip_ids):
    client = bigquery.Client()

    dataset_id = 'BBDD'
    table_id = 'tabla_viajes_1'

    data = []
    for trip_id in trip_ids:
        query = f"""
                SELECT
                    {trip_id} as viaje_id,
                    latitud,
                    longitud
                FROM
                    `{dataset_id}.{table_id}`
                WHERE
                    viaje_id = {trip_id}
                LIMIT 1
                """
        df = client.query(query).to_dataframe()

        # CAMBIOS
        df["cliente_id"] = cliente_ids
        data.append(df)

    return pd.concat(data, ignore_index=True)

def main():
    pubsub_class = PubSubMessages(args.project_id, args.topic_name)
    
    # CAMBIOS
    cliente_ids = [fake.unique.random_int(1, 100000) for _ in range(1)]
    trip_ids = [1, 2, 3]

    try:
        while True:
            df_users = obtener_datos_iniciales(cliente_ids, trip_ids)
            pubsub_class.insert_into_pubsub(pubsub_class, df_users)
            time.sleep(1)  
    except KeyboardInterrupt:
        logging.info("Script interrumpido por el usuario.")
    finally:
        pubsub_class.__exit__()
        logging.info("Cerrando el script de forma ordenada (si es necesario).")

if __name__ == "__main__":
    main()