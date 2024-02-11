import apache_beam as beam
from Funciones import MatchVehiclesAndUsersDoFn
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows

# Variables
project_id = "dataproject-2-413010"
subscription_name_viajes = "dp2_viajes-sub"
subscription_name_clientes = "dp2_clientes-sub"
topic_name_viajes = "dp2_viajes"
topic_name_clientes = "dp2_clientes"
bq_dataset = "BBDD"
bq_table = "pipeline_test1"
bucket_name = "test-dp2"

# Funcion para dar formato

# def decode_message(msg):

#     output = msg.decode('utf-8')

#     logging.info("New PubSub Message: %s", output)

#     return json.loads(output)

def decode_json(element):
    return json.loads(element)

def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        project=project_id,
        runner="DirectRunner"
    )) as p:
        # Lectura de mensajes de vehiculos desde PubSub
        vehicles = (
            p | "ReadFromPubSubViajes" >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_viajes}')
              | "DecodeVehicles" >> beam.Map(decode_json)
              | "WindowViajes" >> beam.WindowInto(FixedWindows(10))  # 10 segundos de ventana
              | 'PairVehicles' >> beam.Map(lambda v: (v['viaje_id'], v))              
        )
        
        # Lectura de mensaje de clientes desde PubSub
        users = (
            p | "ReadFromPubSubClientes" >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_clientes}')
              | "DecodeClientes" >> beam.Map(decode_json)
              | "WindowClientes" >> beam.WindowInto(FixedWindows(10))  # 10 segundos de ventana
              | 'PairUsers' >> beam.Map(lambda u: (u['viaje_id'], u))             
        )
        
        # Combinacion de las coleciones de vehiculos y usuarios
        vehicles_and_users = (
            {'vehicles': vehicles, 'users': users} 
            | 'CombineCollections' >> beam.CoGroupByKey()
        )

        # Procesamiento de las coincidencias entre vehiculos y usuarios
        matches = (
            vehicles_and_users
            | 'MatchVehiclesAndUsers' >> beam.ParDo(MatchVehiclesAndUsersDoFn())
            | 'PrintMatches' >> beam.Map(print)
        )



# def run():
#     vehicles_data = [
#         {"vehicle_id": 125, "viaje_id": 23, "latitud": 39.46829, "longitud": -0.36069, "num_plazas": 2},
#         {"vehicle_id": 126, "viaje_id": 24, "latitud": 39.46830, "longitud": -0.36068, "num_plazas": 2}
#     ]
#     users_data = [
#         {"cliente_id": 33, "viaje_id": 245, "latitud": 39.46827, "longitud": -0.36069},
#         {"cliente_id": 34, "viaje_id": 24, "latitud": 39.46830, "longitud": -0.36068}
#     ]

#     # Definición del pipeline
#     with beam.Pipeline() as p:
#         # Datos de ejemplo
#         vehicles = (p
#                     | 'CreateVehicles' >> beam.Create(vehicles_data)
#                     | 'PairVehicles' >> beam.Map(lambda v: (v['viaje_id'], v)))
#         users = (p
#                  | 'CreateUsers' >> beam.Create(users_data)
#                  | 'PairUsers' >> beam.Map(lambda u: (u['viaje_id'], u)))

#         vehicles_and_users = ({'vehicles': vehicles, 'users': users} 
#                               | 'CombineCollections' >> beam.CoGroupByKey())

#         matches = (vehicles_and_users
#                    | 'MatchVehiclesAndUsers' >> beam.ParDo(MatchVehiclesAndUsersDoFn())
#                    | 'PrintMatches' >> beam.Map(print))


if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")
    
    # Run Process
    run()