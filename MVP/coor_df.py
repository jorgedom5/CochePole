#--------------------------------------------------------------
# MÓDULO: Módulo transformación coordenadas KML a DF
# Descripción: ToDo
#--------------------------------------------------------------

# Importación de librerías

import pandas as pd
from math import radians, sin, cos, sqrt, atan2
import os
import xml.etree.ElementTree as ET

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
                viaje_key = f"viaje {viaje_count}"

                for _, coords in enumerate(coordinates_list):
                    data["viaje"].append(viaje_key)
                    data["punto"].append(_ + 1)
                    data["latitude"].append(coords[1])
                    data["longitude"].append(coords[0])
                    data["altitude"].append(coords[2])

    df = pd.DataFrame(data)
    return df

def calcular_distancias(dataframe): #para calcular las distancias con nuestros datos usando la fórmula anterior (fórmula dentro de fórmula)
    distancia_list = []

    for _ in range(len(dataframe) - 1):
        coord1 = (dataframe["latitude"].iloc[_], dataframe["longitude"].iloc[_], dataframe["altitude"].iloc[_])
        coord2 = (dataframe["latitude"].iloc[_+1], dataframe["longitude"].iloc[_+1], dataframe["altitude"].iloc[_+1])
        distancia = distancia_entre_puntos(coord1, coord2)
        distancia_list.append(distancia)

    distancia_list.append(0)

    dataframe["distancia"] = distancia_list 


#--------------------------------------------------------------
# Submódulo 2: Clase main()
# Descripción: ToDo
#--------------------------------------------------------------

def main():
    kml_folder_path = "data"

    df = kml_df(kml_folder_path)

    calcular_distancias(df)

    print(df.head())
    
    df.to_csv("data.csv", encoding="utf-8", index=False) #IMPORTANTE: QUITAD ESTO, ES SOLO PARA PROBAR Y VER LOS DATOS EN UN CSV
    

if __name__ == "__main__":
    main()
