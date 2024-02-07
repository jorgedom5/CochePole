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

# Iterar sobre los puntos intermedios de la ruta y simular el movimiento del coche
for i, point in enumerate(intermediate_points):
    # Agregar marcador del coche en la ubicación actual
    folium.Marker(location=point, icon=icono_coche).add_to(m)
    
    # Obtener la ubicación de las personas
    ubicacion_persona = [data.iloc[i]["latitud"], data.iloc[i]["longitud"]]
    # Agregar marcador de la persona en la ubicación actual
    folium.Marker(location=ubicacion_persona, icon=folium.Icon(color="orange"), popup=f"Persona {i+1}").add_to(m)
    
    # Mostrar el mapa con Streamlit
    folium_static(m)
    # Eliminar el marcador del coche actual para simular el movimiento
    m = folium.Map(location=[point[0], point[1]], zoom_start=15)
