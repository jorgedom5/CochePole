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
            | 'PrintMatches' >> beam.Map(print)
        )





if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")
    
    # Run Process
    run()