#--------------------------------------------------------------
# MÓDULO: Kafka Producer: envía ubicaciones cada segundo, MVP (1 coordenada de un viaje por segundo)
# Descripción: ToDo
#--------------------------------------------------------------

# Importación de librerías

import time
from confluent_kafka import Producer as ConfluentProducer
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv() # VARIABLES DE ENTORNO

config = {
    'bootstrap.servers': 'kafka:29092',
    'client.id': 'python-producer'
}
topic_name = 'blablacar'

def send_message(producer, message):
    producer.produce(topic=topic_name, value=message)
    producer.flush()

producer = ConfluentProducer(config)  

db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': 5432
}

connection = psycopg2.connect(**db_params)
cursor = connection.cursor()

cursor.execute("SELECT * FROM coordenadas")
rows = cursor.fetchall()

for row in rows:
    coordenada_id, viaje_id, punto_ruta, latitud, longitud, altura, distancia = row

    message = f"Viaje ID: {viaje_id}, Punto Ruta: {punto_ruta}, Latitud: {latitud}, Longitude: {longitud}, Altura: {altura}, Distancia: {distancia}"

    send_message(producer, message)

    time.sleep(1)

cursor.close()
connection.close()

producer.flush()
producer.close()
