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

# Mostrar el mapa con Streamlit
folium_static(m)
