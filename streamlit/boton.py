import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from google.cloud import bigquery
from google.cloud import bigtable
from google.cloud.bigtable import column_family

#client = bigquery.Client(project='dataproject-2-413010')



# Conexión a la instancia de Bigtable
client = bigtable.Client(project='dataproject-2-413010')


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

# Crear desplegable para seleccionar viaje
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
icono_coche = folium.CustomIcon(icon_image="toro-coche.png", icon_size=(32, 32))
# Crear marcador inicial del coche
marker_coche = folium.Marker(location=intermediate_points[0], icon=icono_coche)
marker_coche.add_to(m)

# Definir el ícono personalizado para representar a las personas
icono_persona = folium.CustomIcon(icon_image="streamlit/hombre-1.png", icon_size=(32, 32))
# Definir el umbral de recogida
umbral_de_recogida = 0.0001 
# Inicializar un diccionario para mantener el estado de si cada persona ha sido recogida por el coche o no
personas_recogidas = {i: False for i in range(len(selected_data))}

# Agregar marcadores para las personas en la ruta
marcadores_personas = []
for index, row in selected_data.iterrows():
    marcador_persona = folium.Marker([row['latitud'], row['longitud']], icon=icono_persona)
    marcador_persona.add_to(m)
    marcadores_personas.append(marcador_persona)

# Mostrar el mapa con Streamlit
folium_static(m)

# Botón para actualizar la ubicación del coche
if st.button("Recoger Personas"):
    # Iterar sobre los puntos intermedios de la ruta
    for i, point in enumerate(intermediate_points):
        # Actualizar la ubicación del marcador del coche
        marker_coche.location = point
        
        # Actualizar el estado de las personas que están en el camino
        for j, row in selected_data.iterrows():
            if not personas_recogidas[j]:
                # Calcular la distancia entre el coche y la persona actual
                distancia = folium.distance(row[['latitud', 'longitud']].values, point)
                # Si la distancia es menor a un cierto umbral, marcar a la persona como recogida
                if distancia < umbral_de_recogida:
                    personas_recogidas[j] = True
                    # Eliminar el marcador de la persona del mapa
                    m.remove_layer(marcadores_personas[j])

        # Mostrar el mapa con Streamlit 
        folium_static(m)
