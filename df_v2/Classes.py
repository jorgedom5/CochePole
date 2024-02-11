# Clases Vehicle y Cliente
class Vehicle:
    def __init__(self, vehicle_id, viaje_id, latitud, longitud, num_plazas):
        self.vehicle_id = vehicle_id
        self.viaje_id = viaje_id
        self.latitud = latitud
        self.longitud = longitud
        self.num_plazas = num_plazas
        self.plazas_ocupadas = 0  # Suponiendo que los vehiculos de partida no llevan a nadie

    # Agrega un cliente a un vehiculo y muestra informacion
    def start_journey_with(self, client_id):
        if not self.is_full():
            self.plazas_ocupadas += 1
            plazas_restantes = self.plazas_ocupadas
            print(f"Vehículo {self.vehicle_id} con {self.num_plazas} plazas ha iniciado viaje con cliente {client_id}. Plazas ocupadas: {self.plazas_ocupadas}. Plazas restantes: {plazas_restantes}. Ubicación: {self.latitud}, {self.longitud}")
        
    # Verifica si el vehiculo esta lleno
    def is_full(self):
        return self.plazas_ocupadas >= self.num_plazas
    
    # Verifica si existen plazas disponibles en el vehiculo
    def is_available(self):
        return not self.is_full()

class Cliente:
    def __init__(self, cliente_id, viaje_id, latitud, longitud):
        self.cliente_id = cliente_id
        self.viaje_id = viaje_id
        self.latitud = latitud
        self.longitud = longitud