"""
ESTE 
CÓDIGO
AÚN
ESTÁ
EN
CONSTRUCCIÓN,
GRACIAS

MANDA MENSAJES POR COORDENADAS, ME GUSTARÍA QUE MANDARA EL VIAJE ENTERO.
"""

import pandas as pd
from math import radians, sin, cos, sqrt, atan2
import os
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1
import threading
import argparse
import logging
import secrets
import random
import string
import json
import time


#Input arguments
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

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")


def distancia_entre_puntos(coord1, coord2):
    lat1, lon1, alt1 = map(radians, coord1)
    lat2, lon2, alt2 = map(radians, coord2)

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = 6371000 * c  

    return distance

def kml_df(kml_folder_path):
    data = {"viaje": [], "punto": [], "latitude": [], "longitude": [], "altitude": []}
    viaje_count = 0  

    for _ in os.listdir(kml_folder_path):
        if _.endswith(".kml"):
            kml_file_path = os.path.join(kml_folder_path, _) #ANALIZA TODOS LOS KML DENTRO DE LA CARPETA ESPECIFICADA, EN ESTE CASO data.

            with open(kml_file_path, "r", encoding="utf-8") as file:
                kml_data = file.read()

            root = ET.fromstring(kml_data)
            #para parsear los kml, esto está dentro del kml, más fácil de lo que parece si lees el KML
            coordinates_element = root.find(".//{http://www.opengis.net/kml/2.2}LineString/{http://www.opengis.net/kml/2.2}coordinates")

            if coordinates_element is not None:
                coordinates_str = coordinates_element.text
                coordinates_list = [tuple(map(float, _.split(','))) for _ in coordinates_str.split()]

                viaje_count += 1 
                viaje_key = viaje_count

                for _, coords in enumerate(coordinates_list):
                    data["viaje"].append(viaje_key)
                    data["punto"].append(_ + 1)
                    data["latitude"].append(coords[1])
                    data["longitude"].append(coords[0])
                    data["altitude"].append(coords[2])

    df = pd.DataFrame(data)
    return df

def calcular_distancias(dataframe):
    distancia_list = []

    for i in range(len(dataframe) - 1):
        coord1 = (dataframe["latitude"].iloc[i], dataframe["longitude"].iloc[i], dataframe["altitude"].iloc[i])
        coord2 = (dataframe["latitude"].iloc[i+1], dataframe["longitude"].iloc[i+1], dataframe["altitude"].iloc[i+1])
        distancia = distancia_entre_puntos(coord1, coord2)
        distancia_list.append(distancia)

    distancia_list.append(0)

    dataframe["distancia"] = distancia_list

def asignar_vehiculos_a_viaje(df):
    vehiculos_disponibles = list(df['viaje'].unique())

    for viaje_id in vehiculos_disponibles:
        vehiculo_id = random.randint(1, 1000)
        num_plazas = random.randint(1, 4)

        inicio_viaje = df.loc[df['viaje'] == viaje_id, ['latitude', 'longitude', 'altitude']].iloc[0]

        df.loc[df['viaje'] == viaje_id, 'vehicle_id'] = vehiculo_id
        df.loc[df['viaje'] == viaje_id, 'num_plazas'] = num_plazas
        df.loc[df['viaje'] == viaje_id, 'latitude'] = inicio_viaje['latitude']
        df.loc[df['viaje'] == viaje_id, 'longitude'] = inicio_viaje['longitude']

def insert_into_pubsub(pubsub_class, df):
    for index, row in df.iterrows():
        vehicle_payload = {
            "vehicle_id": int(row["vehicle_id"]),
            "location": str((row["latitude"],row["longitude"])),
            "num_plazas": int(row["num_plazas"]) 
        }
        print(vehicle_payload)
        pubsub_class.publishMessages(vehicle_payload)
        time.sleep(1)

def main():
    kml_folder_path = "data"
    df = kml_df(kml_folder_path)

    asignar_vehiculos_a_viaje(df)

    calcular_distancias(df)

    print(df.head())

    pubsub_class = PubSubMessages(args.project_id, args.topic_name)
    insert_into_pubsub(pubsub_class, df)

if __name__ == "__main__":
    main()
