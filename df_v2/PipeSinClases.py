from datetime import datetime
import apache_beam as beam
import math
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows
from geopy.distance import geodesic
from datetime import timedelta

# Funciones para manejar vehículos
def start_journey_with(vehicle, client_id):
    if not is_full(vehicle):
        vehicle['plazas_ocupadas'] += 1
        vehicle['clientes'].append(client_id)
        timestamp = datetime.now()
        print(f"[{timestamp}] Vehículo {vehicle['vehicle_id']} ha iniciado viaje con cliente {client_id}. Plazas ocupadas: {vehicle['plazas_ocupadas']} y plazas disponibles: {vehicle['num_plazas']}. Ubicación: {vehicle['latitud']}, {vehicle['longitud']}")

def is_full(vehicle):
    return vehicle['plazas_ocupadas'] >= vehicle['num_plazas']

def is_available(vehicle):
    return not is_full(vehicle) and not vehicle['viaje_finalizado']

def actualizar_ubicacion(vehicle, nueva_latitud, nueva_longitud):
    vehicle['latitud'] = nueva_latitud
    vehicle['longitud'] = nueva_longitud
    verificar_finalizacion_viaje(vehicle)

def verificar_finalizacion_viaje(vehicle):
    if vehicle['latitud'] == vehicle['latitud_final'] and vehicle['longitud'] == vehicle['longitud_final']:
        vehicle['viaje_finalizado'] = True
        timestamp = datetime.now()
        print(f"[{timestamp}] Vehículo {vehicle['vehicle_id']} ha finalizado el viaje en {vehicle['latitud']}, {vehicle['longitud']}")

# Función para calcular la distancia
def calculate_distance(lat1, lon1, lat2, lon2):
    location1 = (lat1, lon1)
    location2 = (lat2, lon2)
    distance = geodesic(location1, location2).kilometers
    return distance

def is_within_route(user_location, vehicle_route):
    for point in vehicle_route:
        if calculate_distance(*user_location, *point) <= 20: 
            return True
    return False

# Clase DoFn para emparejar vehículos y usuarios en Apache Beam
class MatchVehiclesAndUsersDoFn(beam.DoFn):
    def process(self, element):     
        viaje_id, collections = element
        vehicles, users = collections['vehicles'], collections['users']

        clientes_emparejados = set()

        for vehicle_data in vehicles:
            vehicle = {
                'vehicle_id': vehicle_data['vehicle_id'],
                'viaje_id': vehicle_data['viaje_id'],
                'latitud': vehicle_data['latitud'],
                'longitud': vehicle_data['longitud'],
                'num_plazas': vehicle_data['num_plazas'],
                'plazas_ocupadas': 0,
                'clientes': [],
                'latitud_final': None,
                'longitud_final': None,
                'viaje_finalizado': False
            }

            for user_data in users:
                cliente = {
                    'cliente_id': user_data['cliente_id'],
                    'viaje_id': user_data['viaje_id'],
                    'latitud': user_data['latitud'],
                    'longitud': user_data['longitud']
                }

                if cliente['cliente_id'] in clientes_emparejados:
                    continue

                user_location = (cliente['latitud'], cliente['longitud'])
                vehicle_location = (vehicle['latitud'], vehicle['longitud'])

                if (is_available(vehicle) and
                    vehicle['viaje_id'] == cliente['viaje_id'] and 
                    is_within_route(user_location, [vehicle_location])):
                    
                    start_journey_with(vehicle, cliente['cliente_id'])
                    clientes_emparejados.add(cliente['cliente_id'])
                    timestamp = datetime.now()
                    yield {
                        'user_id': cliente['cliente_id'],
                        'vehicle_id': vehicle['vehicle_id'],
                        'viaje_id': vehicle['viaje_id'],
                        'latitud': vehicle['latitud'],
                        'longitud': vehicle['longitud'],
                        'timestamp': timestamp.isoformat()
                    }

            if not is_available(vehicle):
                print(f"Vehículo con ID {vehicle['vehicle_id']} en el viaje con ID {viaje_id} está lleno.")


####################################################### PIPELINE #######################################################

# Variables
project_id = "dataproject-2-413010"
subscription_name_viajes = "dp2_viajes-sub"
subscription_name_clientes = "dp2_clientes-sub"
topic_name_viajes = "dp2_viajes"
topic_name_clientes = "dp2_clientes"
bq_dataset = "BBDD"
bq_table = "pipeline_test1"
bucket_name = "test-dp2"

def decode_json(message):

    # Decode PubSub message in order to deal with
    pubsub_message = message.decode('utf-8')
    
    # Convert string decoded in JSON format
    msg = json.loads(pubsub_message)

    # logging.info("New message in PubSub: %s", msg)

    # Return function
    return msg

# def decode_json(element):
#     try:
#         return json.loads(element)
#     except json.JSONDecodeError as e:
#         logging.error(f"Error al decodificar JSON: {e}")
#         return None

def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        project=project_id,
        runner="DirectRunner"
        
        ########################CONFIG PARA ENVIAR A DATAFLOW###############################
        # streaming=True,
        # # save_main_session=True
        # project=project_id,
        # runner="DataflowRunner",
        # temp_location=f"gs://{bucket_name}/tmp",
        # staging_location=f"gs://{bucket_name}/staging",
        # region="europe-west4"
        #######################################################
    )) as p:
        # Lectura de mensajes de vehiculos desde PubSub
        vehicles = (
            p | "ReadFromPubSubViajes" >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_viajes}')
              | "DecodeVehicles" >> beam.Map(decode_json)
              | "WindowViajes" >> beam.WindowInto(FixedWindows(7))  # 10 segundos de ventana
              | 'PairVehicles' >> beam.Map(lambda v: (v['viaje_id'], v))              
        )
        
        # Lectura de mensaje de clientes desde PubSub
        users = (
            p | "ReadFromPubSubClientes" >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_clientes}')
              | "DecodeClientes" >> beam.Map(decode_json)
              | "WindowClientes" >> beam.WindowInto(FixedWindows(7))  # 10 segundos de ventana
              | 'PairUsers' >> beam.Map(lambda u: (u['viaje_id'], u))             
        )


        # # Combinación de colecciones de vehículos y usuarios y procesamiento de emparejamientos
        # vehicles_and_users = (
        #     {'vehicles': vehicles, 'users': users} 
        #     | 'CombineCollections' >> beam.CoGroupByKey()
        #     | 'MatchVehiclesAndUsers' >> beam.ParDo(MatchVehiclesAndUsersDoFn())
        # )
        
        # # Separación de los eventos de emparejamiento
        # emparejamientos = (
        #     vehicles_and_users
        #     | 'FiltrarEmparejamientos' >> beam.Filter(lambda x: 'user_id' in x)
        #     | 'PrintEmparejamientos' >> beam.Map(print)
        # )

        # # Separación de los eventos de finalización de viaje
        # finalizaciones = (
        #     vehicles_and_users
        #     | 'FiltrarFinalizaciones' >> beam.Filter(lambda x: 'viaje_finalizado' in x)
        #     | 'PrintFinalizaciones' >> beam.Map(print)
        # )



        # Combinacion de las coleciones de vehiculos y usuarios
        vehicles_and_users = (
            {'vehicles': vehicles, 'users': users} 
            | 'CombineCollections' >> beam.CoGroupByKey()
        )

        # Procesamiento de las coincidencias entre vehiculos y usuarios
        matches = (
            vehicles_and_users
            | 'MatchVehiclesAndUsers' >> beam.ParDo(MatchVehiclesAndUsersDoFn())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=f"{project_id}:{bq_dataset}.{bq_table}",
                schema="user_id:INTEGER,vehicle_id:INTEGER,viaje_id:INTEGER,latitud:FLOAT,longitud:FLOAT,timestamp:TIMESTAMP",
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )





if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")
    
    # Run Process
    run()

