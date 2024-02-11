from datetime import datetime
import apache_beam as beam
import math
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows


# Clases Vehicle y Cliente
class Vehicle:
    def __init__(self, vehicle_id, viaje_id, latitud, longitud, num_plazas, latitud_final=None, longitud_final=None):
        self.vehicle_id = vehicle_id
        self.viaje_id = viaje_id
        self.latitud = latitud
        self.longitud = longitud
        self.num_plazas = num_plazas
        self.plazas_ocupadas = 0  # Suponiendo que los vehiculos de partida no llevan a nadie
        self.clientes = [] # Lista creadad para ver los clientes que estan dentro del vehiculo
        self.latitud_final = latitud_final
        self.longitud_final = longitud_final
        self.viaje_finalizado = False



    # Agrega un cliente a un vehiculo y muestra informacion
    def start_journey_with(self, client_id):
        if not self.is_full():
            self.plazas_ocupadas += 1
            self.clientes.append(client_id)
            timestamp = datetime.now()  # Obtener el timestamp actual
            print(f"[{timestamp}] Vehículo {self.vehicle_id} ha iniciado viaje con cliente {client_id}. Plazas ocupadas: {self.plazas_ocupadas} y plazas disponibles: {self.num_plazas}. Ubicación: {self.latitud}, {self.longitud}")
        
    def is_full(self):
        return self.plazas_ocupadas >= self.num_plazas
    # Verifica si el vehiculo esta lleno
    def is_available(self):
        return not self.is_full() and not self.viaje_finalizado

    def actualizar_ubicacion(self, nueva_latitud, nueva_longitud):
        self.latitud = nueva_latitud
        self.longitud = nueva_longitud
        self.verificar_finalizacion_viaje()

    def verificar_finalizacion_viaje(self):
        if self.latitud == self.latitud_final and self.longitud == self.longitud_final:
            self.viaje_finalizado = True
            timestamp = datetime.now()  # Obtener el timestamp cuando el viaje finaliza
            print(f"[{timestamp}] Vehículo {self.vehicle_id} ha finalizado el viaje en {self.latitud}, {self.longitud}")

class Cliente:
    def __init__(self, cliente_id, viaje_id, latitud, longitud):
        self.cliente_id = cliente_id
        self.viaje_id = viaje_id
        self.latitud = latitud
        self.longitud = longitud




# Función para calcular la distancia
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Radio de la Tierra en kilómetros
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat/2) * math.sin(dLat/2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dLon/2) * math.sin(dLon/2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c  # Multiplicar por 1000 para convertir a metros

# Función para verificar si la ubicación del usuario está dentro del rango de la ruta del vehículo, depende de "calculate_distance"
def is_within_route(user_location, vehicle_route):
    for point in vehicle_route:
        if calculate_distance(*user_location, *point) <= 1:  # Dentro de 20 metros, lo que es lo mismo a unos 3 mins andando.
            return True
    return False


# Clase DoFn para emparejar vehículos y usuarios en Apache Beam
class MatchVehiclesAndUsersDoFn(beam.DoFn):
    def process(self, element):
        
        viaje_id, collections = element  # Desempaquetar la tupla
        vehicles, users = collections['vehicles'], collections['users'] # Separar las listas de vehículos y usuarios de las collections 

        # Registro de usuarios asignados

        clientes_emparejados = set()

        # Iterar sobre cada vehículo en la lista de vehículos
        for vehicle in vehicles:
            # Crear un objeto Vehicle con los datos del vehículo
            vehicle_obj = Vehicle(
                vehicle_id=vehicle['vehicle_id'],
                viaje_id=vehicle['viaje_id'],
                latitud=vehicle['latitud'],
                longitud=vehicle['longitud'],
                num_plazas=vehicle['num_plazas']
            )
            # Iterar sobre cada usuario en la lista de usuarios
            for user in users:
                # Crear un objeto Cliente con los datos del usuario
                cliente_obj = Cliente(
                    cliente_id=user['cliente_id'],
                    viaje_id=user['viaje_id'],
                    latitud=user['latitud'],
                    longitud=user['longitud']
                )

                # Se verifica si el cliente ya ha sido emparejado SUGERENCIA
                if cliente_obj.cliente_id in clientes_emparejados:
                    continue  # Si tiene viaje, lo salta.



                # Definir las ubicaciones de usuario y vehículo
                user_location = (cliente_obj.latitud, cliente_obj.longitud)
                vehicle_location = (vehicle_obj.latitud, vehicle_obj.longitud)
            
                if (vehicle_obj.is_available() and  
                    vehicle_obj.viaje_id == cliente_obj.viaje_id and 
                    is_within_route(user_location, [vehicle_location])):

                    vehicle_obj.start_journey_with(cliente_obj.cliente_id)
                    clientes_emparejados.add(cliente_obj.cliente_id)

                    # Obtener el timestamp para agregar al output
                    timestamp= datetime.now()
                    yield {'user_id': cliente_obj.cliente_id, 'vehicle_id': vehicle_obj.vehicle_id, 'viaje_id': vehicle_obj.viaje_id, 'latitud':vehicle_obj.latitud, 'longitud':vehicle_obj.longitud, 'timestamp': timestamp.isoformat()}

            if not vehicle_obj.is_available():
                print(f"Vehículo con ID {vehicle_obj.vehicle_id} en el viaje con ID {viaje_id} está lleno.")
            else:
                print(f"No hay clientes para el vehículo con ID {vehicle_obj.vehicle_id} en el viaje con ID {viaje_id}")




# Variables
project_id = "dataproject-2-413010"
subscription_name_viajes = "dp2_viajes-sub"
subscription_name_clientes = "dp2_clientes-sub"
topic_name_viajes = "dp2_viajes"
topic_name_clientes = "dp2_clientes"
bq_dataset = "BBDD"
bq_table = "pipeline_test1"
bucket_name = "test-dp2"

# def decode_json(element):
#     return json.loads(element)
def decode_json(message):

    # Decode PubSub message in order to deal with
    pubsub_message = message.decode('utf-8')
    
    # Convert string decoded in JSON format
    msg = json.loads(pubsub_message)

    logging.info("New message in PubSub: %s", msg)

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
    )) as p:
        # Lectura de mensajes de vehiculos desde PubSub
        vehicles = (
            p | "ReadFromPubSubViajes" >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_viajes}')
              | "DecodeVehicles" >> beam.Map(decode_json)
              | "WindowViajes" >> beam.WindowInto(FixedWindows(5))  # 10 segundos de ventana
              | 'PairVehicles' >> beam.Map(lambda v: (v['viaje_id'], v))              
        )
        
        # Lectura de mensaje de clientes desde PubSub
        users = (
            p | "ReadFromPubSubClientes" >> ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name_clientes}')
              | "DecodeClientes" >> beam.Map(decode_json)
              | "WindowClientes" >> beam.WindowInto(FixedWindows(5))  # 10 segundos de ventana
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
                    