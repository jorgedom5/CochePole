import apache_beam as beam
from beam_pipeline import MatchVehiclesAndUsersDoFn

def run():
    vehicles_data = [
        {"vehicle_id": 125, "viaje_id": 23, "latitud": 39.46829, "longitud": -0.36069, "num_plazas": 2},
        {"vehicle_id": 126, "viaje_id": 24, "latitud": 39.46830, "longitud": -0.36068, "num_plazas": 2}
    ]
    users_data = [
        {"cliente_id": 33, "viaje_id": 245, "latitud": 39.46827, "longitud": -0.36069},
        {"cliente_id": 34, "viaje_id": 24, "latitud": 39.46830, "longitud": -0.36068}
    ]

    # Definición del pipeline
    with beam.Pipeline() as p:
        # Datos de ejemplo
        vehicles = (p
                    | 'CreateVehicles' >> beam.Create(vehicles_data)
                    | 'PairVehicles' >> beam.Map(lambda v: (v['viaje_id'], v)))
        users = (p
                 | 'CreateUsers' >> beam.Create(users_data)
                 | 'PairUsers' >> beam.Map(lambda u: (u['viaje_id'], u)))

        vehicles_and_users = ({'vehicles': vehicles, 'users': users} 
                              | 'CombineCollections' >> beam.CoGroupByKey())

        matches = (vehicles_and_users
                   | 'MatchVehiclesAndUsers' >> beam.ParDo(MatchVehiclesAndUsersDoFn())
                   | 'PrintMatches' >> beam.Map(print))


if __name__ == '__main__':

    # Set Logs
    # logging.getLogger().setLevel(logging.INFO)

    # logging.info("The process started")
    
    # Run Process
    run()