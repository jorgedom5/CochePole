import pandas as pd
from math import radians, sin, cos, sqrt, atan2
import os
import xml.etree.ElementTree as ET
from google.cloud import bigquery

# Submódulo 1: Funciones

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
            kml_file_path = os.path.join(kml_folder_path, _)  # ANALIZA TODOS LOS KML DENTRO DE LA CARPETA ESPECIFICADA, EN ESTE CASO data.

            with open(kml_file_path, "r", encoding="utf-8") as file:
                kml_data = file.read()

            root = ET.fromstring(kml_data)
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

def create_bigquery_table(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    schema = [
        bigquery.SchemaField("viaje_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("punto_ruta", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("latitud", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("longitud", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("altura", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("distancia", "FLOAT", mode="REQUIRED"),
    ]

    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    try:
        table = client.create_table(table)  # API request
        print("Tabla creada: {}".format(table.table_id))
    except:
        print("La tabla ya existe.")

def insert_into_bigquery(project_id, dataset_id, table_id, rows):
    client = bigquery.Client(project=project_id)

    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    errors = client.insert_rows(table, rows)  # API request
    if errors:
        print("Error al insertar filas: {}".format(errors))
    else:
        print("Filas insertadas con éxito.")

def insert_into_bigquery_wrapper(df, project_id, dataset_id, table_id):
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # Asegúrate de que la tabla exista antes de insertar datos
    create_bigquery_table(project_id, dataset_id, table_id)

    # Inserta datos en la tabla de BigQuery
    insert_into_bigquery(project_id, dataset_id, table_id, rows)

# Submódulo 2: Clase main()

def main():
    kml_folder_path = "data"
    project_id = "dataproject-2-413010"
    dataset_id = "BBDD"
    table_id = "tabla_viajes_1"

    df = kml_df(kml_folder_path)

    calcular_distancias(df)

    print(df.head())

    # Conectar a BigQuery y guardar los datos
    insert_into_bigquery_wrapper(df, project_id, dataset_id, table_id)

if __name__ == "__main__":
    main()
