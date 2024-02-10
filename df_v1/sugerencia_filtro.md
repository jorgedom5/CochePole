# SUGERENCIA FILTRAR CLIENTES YA SUBIDOS, Y NÚMERO DE PLAZAS

## EN Funciones.py

Antes de las funciones, después de importar las librerías, añade:

```python
clientes_emparejados = []
````

Luego, en la clase del DoFn, prueba (busca la anotación SUGERENCIA para encontrar que he añadido)

```python
class MatchVehiclesAndUsersDoFn(beam.DoFn):
    def process(self, element):
        viaje_id, collections = element  # Desempaquetar la tupla
        vehicles, users = collections['vehicles'], collections['users']  # Separar las listas de vehículos y usuarios de las collections

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

                # Comprobar si el vehículo está disponible, si el viaje_id coincide y si el usuario está en la ruta del vehículo
                if (vehicle_obj.is_available and
                        vehicle_obj.viaje_id == cliente_obj.viaje_id and
                        is_within_route(user_location, [vehicle_location])):
                    # Iniciar el viaje con el usuario y generar el resultado
                    vehicle_obj.start_journey_with(cliente_obj.cliente_id)
                    clientes_emparejados.append(cliente_obj.cliente_id)  # SUGERENCIA, añade el cliente nuevo emparejado a la lista
                    yield {'user_id': cliente_obj.cliente_id, 'vehicle_id': vehicle_obj.vehicle_id,
                           'viaje_id': vehicle_obj.viaje_id, 'latitud': vehicle_obj.latitud,
                           'longitud': vehicle_obj.longitud}
                    break  # Romper el bucle interno una vez que se encuentra una coincidencia para que no busque más clientes
            else:
                print(f"No hay clientes para el vehículo con ID {vehicle_obj.vehicle_id} en el viaje con ID {viaje_id}")
```

## PARA LOS NUMERO DE PLAZAS (No estoy seguro de esta parte, es un primer boceto)

Modifica esta parte del código, donde se añade la variable num_plazas a partir del vehicle

```python
if (vehicle_obj.is_available and
                        vehicle_obj.viaje_id == cliente_obj.viaje_id and
                        is_within_route(user_location, [vehicle_location]) and
                        plazas_restantes > 0):  # Verificar si hay plazas disponibles
                    # Iniciar el viaje con el usuario y generar el resultado
                    vehicle_obj.start_journey_with(cliente_obj.cliente_id)
                    clientes_emparejados.append(cliente_obj.cliente_id)  # Agregar cliente emparejado a la lista
                    plazas_restantes -= 1  # Restar una plaza al vehículo

                    yield {'user_id': cliente_obj.cliente_id, 'vehicle_id': vehicle_obj.vehicle_id,
                           'viaje_id': vehicle_obj.viaje_id, 'latitud': vehicle_obj.latitud,
                           'longitud': vehicle_obj.longitud}
                # ASÍ MIENTRAS HAYAN PLAZAS
            else:
                if plazas_restantes == vehicle_obj.num_plazas:
                    print(f"No hay clientes para el vehículo con ID {vehicle_obj.vehicle_id} en el viaje con ID {viaje_id}")
                else:
                    print(f"No hay más plazas disponibles en el vehículo con ID {vehicle_obj.vehicle_id} en el viaje con ID {viaje_id}. Quedan {plazas_restantes} plazas.")

```