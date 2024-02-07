import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from google.cloud import bigquery

# Especifica la ruta del archivo JSON de las credenciales
credenciales = "/Users/balmagostr/Documents/GitHub/CochePole/streamlit/credenciales.json"

# Carga las credenciales
client = bigquery.Client.from_service_account_json(credenciales)

# Consulta SQL para obtener datos de BigQuery
query = """
SELECT *
FROM dataproject-2-413010.BBDD.tabla_viajes_1
"""

# Obtener los datos de BigQuery
@st.cache
def load_data():
    return client.query(query).to_dataframe()

data = load_data()

# Obtener los IDs únicos de los viajes
viaje_ids = data["viaje_id"].unique()

# Sidebar para seleccionar el viaje
selected_viaje = st.sidebar.selectbox("Selecciona el viaje:", viaje_ids)

# Filtrar los datos por el viaje seleccionado
selected_data = data[data["viaje_id"] == selected_viaje]

# Crear un mapa con Folium
st.title(f"Ruta del viaje {selected_viaje}")
m = folium.Map(location=[selected_data["latitud"].mean(), selected_data["longitud"].mean()], zoom_start=15)

# Agregar marcador al inicio de la ruta
inicio = [selected_data.iloc[0]["latitud"], selected_data.iloc[0]["longitud"]]
folium.Marker(location=inicio, icon=folium.Icon(color="green"), popup="Inicio").add_to(m)

# Agregar marcador al final de la ruta
final = [selected_data.iloc[-1]["latitud"], selected_data.iloc[-1]["longitud"]]
folium.Marker(location=final, icon=folium.Icon(color="red"), popup="Final").add_to(m)

# Crear una línea que conecte los puntos intermedios de la ruta
intermediate_points = selected_data.iloc[1:-1][["latitud", "longitud"]].values.tolist()
folium.PolyLine(locations=intermediate_points, color='blue', weight=2).add_to(m)

# Definir el icono personalizado para representar el coche
icono_coche = folium.CustomIcon(icon_image="https://image.flaticon.com/icons/png/512/2990/2990506.png", icon_size=(32, 32))

# Conjunto para realizar un seguimiento de las personas que ya han sido encontradas y eliminadas por el coche
personas_encontradas = set()

# Iterar sobre los puntos intermedios de la ruta y simular el movimiento del coche
for i, point in enumerate(intermediate_points):
    # Eliminar el marcador del coche de la iteración anterior, si existe
    if i > 0:
        m.get_root().html.add_child(folium.Element(f'<script>document.getElementsByClassName("leaflet-marker-icon leaflet-zoom-animated leaflet-interactive")[0].remove();</script>'))
    
    # Agregar marcador del coche en la ubicación actual
    folium.Marker(location=point, icon=icono_coche).add_to(m)
    
    # Obtener la ubicación actual del coche
    ubicacion_coche = point
    
    # Obtener las ubicaciones de las personas en esta iteración
    personas_presentes = []
    for persona in range(1, 5):
        latitud_persona = selected_data.iloc[i][f"latitud_persona_{persona}"]
        longitud_persona = selected_data.iloc[i][f"longitud_persona_{persona}"]
        if not pd.isnull(latitud_persona) and not pd.isnull(longitud_persona):
            ubicacion_persona = (latitud_persona, longitud_persona)
            # Verificar si el coche está en la misma ubicación que alguna persona
            if ubicacion_coche == ubicacion_persona:
                # Si el coche está en la misma ubicación que una persona, eliminar el marcador de esa persona
                personas_encontradas.add(ubicacion_persona)
            else:
                personas_presentes.append(ubicacion_persona)
    
    # Agregar marcadores de las personas presentes al mapa, excepto las personas encontradas
    for ubicacion_persona in personas_presentes:
        folium.Marker(location=ubicacion_persona, icon=folium.Icon(color="orange"), popup="Persona").add_to(m)

# Mostrar el mapa con Streamlit fuera del bucle
folium_static(m)
