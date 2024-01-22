#--------------------------------------------------------------
# MÓDULO: Módulo transformación coordenadas KML a DF
# Descripción: ToDo
#--------------------------------------------------------------

# Importación de librerías

import pandas as pd
from math import radians, sin, cos, sqrt, atan2
import os
import xml.etree.ElementTree as ET
import psycopg2
from dotenv import load_dotenv 

#--------------------------------------------------------------
# Submódulo 1: Funciones
# Descripción: ToDo
#--------------------------------------------------------------

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
        distancia = distancia_entre_puntos(coord1, coord2) #para calcular las distancias con nuestros datos usando la fórmula anterior (fórmula dentro de fórmula)
        distancia_list.append(distancia)

    distancia_list.append(0)

    dataframe["distancia"] = distancia_list
    
def insert_into_postgres(conn, df):
    cursor = conn.cursor()

    for index, row in df.iterrows():
        
        cursor.execute("""
            INSERT INTO viajes (viaje_id, ruta)
            VALUES (%s, %s)
            ON CONFLICT (viaje_id) DO NOTHING
        """, (
            row["viaje"],
            f"Ruta {row['viaje']}"  
        ))
        
        cursor.execute("""
            INSERT INTO coordenadas (viaje_id, punto_ruta, latitud, longitud, altura, distancia)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row["viaje"],
            row["punto"],
            row["latitude"],
            row["longitude"],
            row["altitude"],
            row["distancia"]
        ))
            

    conn.commit()
    cursor.close()
    

#--------------------------------------------------------------
# Submódulo 2: Clase main()
# Descripción: ToDo
#--------------------------------------------------------------

def main():
    load_dotenv()
    kml_folder_path = "/app/data" # "/app/data" CON DOCKER, "data" A SECAS SIN, en cloud será a secas tb supongo


    df = kml_df(kml_folder_path)

    calcular_distancias(df)

    print(df.head())
    
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    insert_into_postgres(conn, df)

    conn.close()

if __name__ == "__main__":
    main()
