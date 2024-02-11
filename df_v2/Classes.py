from datetime import datetime
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