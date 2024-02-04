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


"""#Input arguments
parser = argparse.ArgumentParser(description=('User Data Generator'))

parser.add_argument(
    '--project_id',
    required=True,
    help='GCP cloud project name.')
parser.add_argument(
    '--topic_name',
    required=True,
    help='PubSub topic name.')

args, opts = parser.parse_known_args()"""

#Creamos cliente y especificamos origen de los datos
def obtener_viaje_aleatorio():
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
                    longitud
                FROM
                    `{dataset_id}.{table_id}`
                ORDER BY
                    RAND()
                LIMIT 1
            """
        df_users = client.query(query).to_dataframe()

        for index , row in df_users.iterrows():
            print(f"Viaje ID: {row['viaje_id']}, Latitud: {row['latitud']}, Longitud: {row['longitud']}")
    except Exception as e:
        logging.error(f"Error al obtener viaje aleatorio: {str(e)}")


# Ejecuta la consulta cada 5 segundos
try:
    while True:
        obtener_viaje_aleatorio()
        time.sleep(5)
except KeyboardInterrupt:
        logging.info("Script interrumpido por el usuario.")
finally:
    logging.info("Cerrando el script de forma ordenada (si es necesario).")



