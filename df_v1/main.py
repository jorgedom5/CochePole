import apache_beam as beam
from Funciones import MatchVehiclesAndUsersDoFn
import json
import logging

# Variables
project_id = "dataproject-2-413010"
subscription_name = "sub_dp2"
topic_name= "topic_dp2"
bq_dataset = "BBDD"
bq_table = "test3"
bucket_name = "test-dp2"

# Funcion para dar formato

def decode_message(msg):

    output = msg.decode('utf-8')

    logging.info("New PubSub Message: %s", output)

    return json.loads(output)


def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        # save_main_session=True
        project=project_id,
        runner="DataflowRunner",
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="us-central1"
    )) as p:
        (

            
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