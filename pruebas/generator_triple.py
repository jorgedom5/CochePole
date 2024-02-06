import pandas as pd
import os
from google.cloud import pubsub_v1
import threading
import argparse
import logging
import secrets
import random
import string
import json
import time
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor

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

args, opts = parser.parse_known_args()


from google.cloud import bigquery

client = bigquery.Client()

dataset_id = 'BBDD'
table_id = 'tabla_viajes_1'

table_ref = client.dataset(dataset_id).table(table_id)
table = client.get_table(table_ref)


query = f"SELECT * FROM `{table.project}.{table.dataset_id}.{table.table_id}`"
df = client.query(query).to_dataframe()


""" Helpful Code """

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
        logging.info("A New vehicle has been monitored. Id: %s", message['vehicle_id'])
        print(f"Published message: {json_str}")

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

def asignar_vehiculos_a_viaje(df, viaje_id):
    vehiculo_id = random.randint(1, 1000)
    num_plazas = random.randint(1, 4)

    df.loc[df['viaje_id'] == viaje_id, 'vehicle_id'] = vehiculo_id
    df.loc[df['viaje_id'] == viaje_id, 'num_plazas'] = num_plazas

def insert_into_pubsub(pubsub_class, df, viaje_id, cancel_event):
    trip_rows = df[df['viaje_id'] == viaje_id]

    if trip_rows.empty:
        print(f"Error viaje_id: {viaje_id}")
        return

    for index, row in trip_rows.iterrows():
        # Obtener las coordenadas del punto de ruta actual
        vehicle_payload = {
            "viaje_id": int(row["viaje_id"]),
            "vehicle_id": int(row["vehicle_id"]),
            "location": str((row['latitud'], row['longitud'])),
            "num_plazas": int(row["num_plazas"])
        }

        pubsub_class.publishMessages(vehicle_payload)

        # Check for cancellation
        if cancel_event.is_set():
            print("Thread canceled.")
            return

        time.sleep(0.3)

def process_trip(pubsub_class, df, cancel_event):
    while not cancel_event.is_set():
        # Randomly select a viaje_id from the unique values
        viaje_id = random.choice(df['viaje_id'].unique())

        # Reiniciar el conteo de vehículos para el nuevo viaje
        df.loc[df['viaje_id'] != viaje_id, 'vehicle_id'] = None
        df.loc[df['viaje_id'] != viaje_id, 'num_plazas'] = None

        # Asignar vehículos al viaje seleccionado
        asignar_vehiculos_a_viaje(df, viaje_id)

        # Insertar datos en PubSub para el viaje seleccionado
        insert_into_pubsub(pubsub_class, df, viaje_id, cancel_event)

        # Sleep for a short duration to simulate time passing between trips
        time.sleep(1)

def main():
    pubsub_class = PubSubMessages(args.project_id, args.topic_name)

    # Create an event for cancellation
    cancel_event = threading.Event()

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []

        try:
            # Submit a task for each thread
            for _ in range(3):
                future = executor.submit(process_trip, pubsub_class, df.copy(), cancel_event)
                futures.append(future)

            # Wait for all submitted tasks to finish
            for future in futures:
                future.result()

        except KeyboardInterrupt:
            # Handle keyboard interrupt
            print("Keyboard interrupt received. Stopping threads.")

            # Set the cancellation event for all threads
            cancel_event.set()

            # Wait for the threads to complete or be canceled
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    pass  # Ignore exceptions raised by canceled threads

            # Close PubSub client
            pubsub_class.__exit__()

if __name__ == "__main__":
    main()
