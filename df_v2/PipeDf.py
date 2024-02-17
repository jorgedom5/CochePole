import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows



# Clase DoFn para emparejar vehiculos y usuarios en Apache Beam
class MatchVehiculosAndUsersDoFn(beam.DoFn):
    
    def is_full(self,vehiculo):
        return vehiculo['plazas_ocupadas'] >= vehiculo['num_plazas']



    def is_available(self,vehiculo):
        return not self.is_full(vehiculo) and not vehiculo['viaje_finalizado']
    

    def is_within_route(self, user_location, vehiculo_route):
        for point in vehiculo_route:
            if self.calculate_distance(*user_location, *point) <= 2:  
                return True
        return False
    

    def calculate_distance(self,lat1, lon1, lat2, lon2):
        from geopy.distance import geodesic 
        location1 = (lat1, lon1)
        location2 = (lat2, lon2)
        distance = geodesic(location1, location2).kilometers
        return distance



    def start_journey_with(self, vehiculo, client_id):
        from datetime import datetime
        if not self.is_full(vehiculo):
            vehiculo['plazas_ocupadas'] += 1
            vehiculo['clientes'].append(client_id)
            timestamp = datetime.now()
            print(f"[{timestamp}] Vehiculo {vehiculo['vehiculo_id']} ha iniciado viaje con cliente {client_id}. Plazas ocupadas: {vehiculo['plazas_ocupadas']}\
                y plazas disponibles: {vehiculo['num_plazas']}. Ubicación: {vehiculo['latitud']}, {vehiculo['longitud']}")

    def process(self, element):
        from datetime import datetime
        import logging
        #print(element)     
        viaje_id, collections = element
        vehiculos, users = collections['vehiculos'], collections['users']

        clientes_emparejados = set()
        vehiculos_procesados = set() #agregado

        for vehiculo_data in vehiculos:
            if vehiculo_data['vehiculo_id'] in vehiculos_procesados: #agregado
                continue
            try:
                vehiculo = {
                    'vehiculo_id': vehiculo_data['vehiculo_id'],
                    'viaje_id': vehiculo_data['viaje_id'],
                    'latitud': vehiculo_data['latitud'],
                    'longitud': vehiculo_data['longitud'],
                    'num_plazas': vehiculo_data['num_plazas'],
                    'latitud_final': vehiculo_data['latitud_final'],
                    'longitud_final': vehiculo_data['longitud_final'],
                    'plazas_ocupadas': 0,
                    'clientes': [],                
                    'viaje_finalizado': False
                }

                for user_data in users:
                    cliente = {
                        'cliente_id': user_data['cliente_id'],
                        'viaje_id': user_data['viaje_id'],
                        'latitud': user_data['latitud'],
                        'longitud': user_data['longitud'],
                        'rating': user_data['rating'],
                        'metodo_pago': user_data['metodo_pago']
                    }

                    if cliente['cliente_id'] in clientes_emparejados:
                        continue
                    
                    user_location = (cliente['latitud'], cliente['longitud'])
                    vehiculo_location = (vehiculo['latitud'], vehiculo['longitud'])

                    if (self.is_available(vehiculo) and
                        vehiculo['viaje_id'] == cliente['viaje_id'] and 
                        self.is_within_route(user_location, [vehiculo_location]) and
                        cliente['cliente_id'] not in vehiculo['clientes']):
                        
                        self.start_journey_with(vehiculo, cliente['cliente_id'])
                        clientes_emparejados.add(cliente['cliente_id'])

                        # Calcular la distancia y el pago del viaje
                        distancia = self.calculate_distance(vehiculo['latitud'], vehiculo['longitud'], vehiculo['latitud_final'], vehiculo['longitud_final'])
                        pago_viaje = distancia * 1  # 1 euro por kilómetro


                        timestamp = datetime.now()
                        yield {
                            'cliente_id': cliente['cliente_id'],###### CORREGIR EN BQ
                            'rating': cliente['rating'],
                            'metodo_pago': cliente['metodo_pago'],
                            'pago_viaje': pago_viaje,####AQUI VA EL PAGO_VIAJE
                            'viaje_id': vehiculo['viaje_id'],
                            'vehiculo_id': vehiculo['vehiculo_id'],                                                        
                            'latitud': vehiculo['latitud'],
                            'longitud': vehiculo['longitud'],
                            'latitud_final':vehiculo['latitud_final'],
                            'longitud_final':vehiculo['longitud_final'],
                            'timestamp': timestamp.isoformat()
                        }
                        if self.is_full(vehiculo):
                            vehiculos_procesados.add(vehiculo['vehiculo_id']) # agregado

            except KeyError as e:
                logging.error(f"Falta una clave esperada en los datos del vehiculo: {e}")

            if not self.is_available(vehiculo):
                print(f"Vehiculo con ID {vehiculo['vehiculo_id']} en el viaje con ID {viaje_id} está lleno.")

####################################################### PIPELINE #######################################################

# Variables
project_id = "dataproject-2-413010"
subscription_name_viajes = "dp2_viajes-sub"
subscription_name_clientes = "dp2_clientes-sub"
topic_name_viajes = "dp2_viajes"
topic_name_clientes = "dp2_clientes"
bq_dataset = "BBDD"
bq_table = "CochePole_BD"
bucket_name = "test-dp2"

def decode_json(message):

    # Decode PubSub message in order to deal with
    pubsub_message = message.decode('utf-8')
    
    # Convert string decoded in JSON format
    msg = json.loads(pubsub_message)

    # logging.info("New message in PubSub: %s", msg)

    # Return function
    return msg

def run():
    with beam.Pipeline(options=PipelineOptions(        
        streaming=True,
        # save_main_session=True
        project=project_id,
        runner="DataflowRunner",
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="europe-west4"
        ######################################################
    )) as p:
        # Lectura de mensajes de vehiculos desde PubSub
        vehiculos = (
            p | "ReadFromPubSubViajes" >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_viajes}')
              | "DecodeVehiculos" >> beam.Map(decode_json)
              | "WindowViajes" >> beam.WindowInto(FixedWindows(30))  # 10 segundos de ventana
              | 'PairVehiculos' >> beam.Map(lambda v: (v['viaje_id'], v))              
        )
        
        # Lectura de mensaje de clientes desde PubSub
        users = (
            p | "ReadFromPubSubClientes" >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_clientes}')
              | "DecodeClientes" >> beam.Map(decode_json)
              | "WindowClientes" >> beam.WindowInto(FixedWindows(30))  # 10 segundos de ventana
              | 'PairClientes' >> beam.Map(lambda u: (u['viaje_id'], u))             
        )   

        # Combinacion de las coleciones de vehiculos y usuarios ***** CON BQ *****
        vehiculos_and_users = (
            {'vehiculos': vehiculos, 'users': users} 
            | 'CombineCollections' >> beam.CoGroupByKey()
        )

        # Procesamiento de las coincidencias entre vehiculos y usuarios
        matches = (
            vehiculos_and_users
            | 'MatchVehiculosAndUsers' >> beam.ParDo(MatchVehiculosAndUsersDoFn())
            | 'WriteToBigQuery' >> WriteToBigQuery(
                table=f"{project_id}:{bq_dataset}.{bq_table}",
                schema="cliente_id:INTEGER,rating:FLOAT,metodo_pago:STRING,pago_viaje:FLOAT,viaje_id:INTEGER,vehiculo_id:INTEGER,latitud:FLOAT,longitud:FLOAT,latitud_final:FLOAT,longitud_final:FLOAT,timestamp:TIMESTAMP",
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
