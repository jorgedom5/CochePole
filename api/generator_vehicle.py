import pandas as pd
from google.cloud import pubsub_v1
import argparse
import logging
import random
import json
import time
from google.cloud import bigquery
import concurrent.futures
import threading

# Input arguments
parser = argparse.ArgumentParser(description=('Vehicle Data Generator'))

parser.add_argument(
    '--project_id',
    required=True,
    help='GCP cloud project name.')
parser.add_argument(
    '--topic_name',
    required=True,
    help='PubSub topic name.')

args = parser.parse_args() 

client = bigquery.Client()
dataset_id = 'BBDD'
table_id = 'tabla_viajes_1'

table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)

query = f"SELECT * FROM `{table.project}.{table.dataset_id}.{table.table_id}`"
df = client.query(query).to_dataframe()


class PubSubMessages:

    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id: str, topic_name: str):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message: str):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("A New vehicle has been monitored. Id: %s", message['vehiculo_id'])
        print(f"Published message: {json_str}")

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")


def insert_into_pubsub(pubsub_class, df):
    def process_trip(viaje_id_random, df_copy):
        for viaje_id in df_copy['viaje_id'].unique():
            vehiculo_id = random.randint(1, 1000)
            num_plazas = random.randint(1, 4)

            df_copy.loc[df_copy['viaje_id'] == viaje_id, 'vehiculo_id'] = vehiculo_id
            df_copy.loc[df_copy['viaje_id'] == viaje_id, 'num_plazas'] = num_plazas

        trip_rows = df_copy[df_copy['viaje_id'] == viaje_id_random]

        if trip_rows.empty:
            print("Error viaje_id.")
            return

        latitud_final = float(trip_rows['latitud'].max())
        longitud_final = float(trip_rows['longitud'].max())
        last_coordinates_row = trip_rows.loc[trip_rows['punto_ruta'].idxmax()]

        for index, row in trip_rows.iterrows():
            vehicle_payload = {
                "viaje_id": int(row["viaje_id"]),
                "vehiculo_id": int(row["vehiculo_id"]),
                "latitud": float(row["latitud"]),
                "longitud": float(row["longitud"]),
                "num_plazas": int(row["num_plazas"]),
                "latitud_final": float(last_coordinates_row["latitud"]),
                "longitud_final": float(last_coordinates_row["longitud"])
            }

            pubsub_class.publishMessages(vehicle_payload)
            time.sleep(7)

    threads = []
    viaje_ids_random = [random.randint(1, 38) for _ in range(60)]  # NUMERO DE COCHES A LA VEZ
    for viaje_id in viaje_ids_random:
        thread = threading.Thread(target=process_trip, args=(viaje_id, df.copy()))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


def main():
    pubsub_class = PubSubMessages(args.project_id, args.topic_name)

    try:
        insert_into_pubsub(pubsub_class, df)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt detected. Stopping the program gracefully.")
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    main()
