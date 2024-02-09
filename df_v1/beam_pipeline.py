import apache_beam as beam
from Classes import Vehicle, Cliente
from Utilidades import is_within_route


# Clase DoFn para emparejar vehículos y usuarios en Apache Beam
class MatchVehiclesAndUsersDoFn(beam.DoFn):
    def process(self, element):
        
        viaje_id, collections = element  # Desempaquetar la tupla
        vehicles, users = collections['vehicles'], collections['users'] # Separar las listas de vehículos y usuarios de las collections 


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

                # Definir las ubicaciones de usuario y vehículo
                user_location = (cliente_obj.latitud, cliente_obj.longitud)
                vehicle_location = (vehicle_obj.latitud, vehicle_obj.longitud)

                # Comprobar si el vehículo está disponible, si el viaje_id coincide y si el usuario está en la ruta del vehículo
                if (vehicle_obj.is_available and 
                    vehicle_obj.viaje_id == cliente_obj.viaje_id and 
                    is_within_route(user_location, [vehicle_location])):  # Asegurándose de que vehicle_route sea una lista
                    
                     # Iniciar el viaje con el usuario y generar el resultado
                    vehicle_obj.start_journey_with(cliente_obj.cliente_id)
                    yield {'user_id': cliente_obj.cliente_id, 'vehicle_id': vehicle_obj.vehicle_id, 'latitud':vehicle_obj.latitud, 'longitud':vehicle_obj.longitud}
                    break # Romper el bucle interno una vez que se encuentra una coincidencia para que no busque mas clientes