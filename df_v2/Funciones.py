import apache_beam as beam
from Classes import Vehicle, Cliente
import math
import datetime as datetime

# Función para calcular la distancia
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Radio de la Tierra en kilómetros
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat/2) * math.sin(dLat/2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dLon/2) * math.sin(dLon/2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c * 1000 # Multiplicar por 1000 para convertir a metros

# Función para verificar si la ubicación del usuario está dentro del rango de la ruta del vehículo, depende de "calculate_distance"
def is_within_route(user_location, vehicle_route):
    for point in vehicle_route:
        if calculate_distance(*user_location, *point) <= 2000:  # Dentro de 20 metros, lo que es lo mismo a unos 3 mins andando.
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
                    