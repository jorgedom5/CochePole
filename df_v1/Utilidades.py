import math

# Función para calcular la distancia
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Radio de la Tierra en kilómetros
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat/2) * math.sin(dLat/2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dLon/2) * math.sin(dLon/2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# Función para verificar si la ubicación del usuario está dentro del rango de la ruta del vehículo, depende de "calculate_distance"
def is_within_route(user_location, vehicle_route):
    for point in vehicle_route:
        if calculate_distance(*user_location, *point) <= 1:  # Dentro de 1 km
            return True
    return False