# Clases Vehicle y Cliente
class Vehicle:
    def __init__(self, vehicle_id, viaje_id, latitud, longitud, num_plazas):
        self.vehicle_id = vehicle_id
        self.viaje_id = viaje_id
        self.latitud = latitud
        self.longitud = longitud
        self.num_plazas = num_plazas
        self.is_available = True  # Suponiendo que todos los vehículos están inicialmente disponibles

    def start_journey_with(self, client_id):
        self.is_available = False
        print(f"Vehículo {self.vehicle_id} con {self.num_plazas} plazas ha iniciando viaje con cliente {client_id} y fue recogido en la ubicacion {self.latitud} {self.longitud}")

class Cliente:
    def __init__(self, cliente_id, viaje_id, latitud, longitud):
        self.cliente_id = cliente_id
        self.viaje_id = viaje_id
        self.latitud = latitud
        self.longitud = longitud