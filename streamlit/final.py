import streamlit as st
import pandas as pd
import folium
import random
import time
from streamlit_folium import folium_static
from google.cloud import bigquery

client = bigquery.Client(project='dataproject-2-413010')

# Consulta SQL para obtener datos de BigQuery
query_viajes = """
SELECT *
FROM dataproject-2-413010.BBDD.tabla_viajes_1
"""

# Consulta SQL para obtener datos de la tabla simulador_streamlit
query_personas = """
SELECT latitud, longitud, viaje_id
FROM dataproject-2-413010.BBDD.simulador_streamlit2
"""

# Obtener los datos de BigQuery
@st.cache_data
def load_data():
    return client.query(query_viajes).to_dataframe(), client.query(query_personas).to_dataframe()

# Obtener los datos de las tablas de viajes y personas
data_viajes, data_personas = load_data()

# Obtener la lista de IDs de viajes únicos
viaje_ids = data_viajes["viaje_id"].unique()

# Obtener las imágenes predeterminadas de las personas para el mapa general
imagenes_predeterminadas = {}  # Un diccionario para almacenar las imágenes predeterminadas por persona
for index, row in data_personas.iterrows():
    imagenes_predeterminadas[(row['latitud'], row['longitud'])] = random.choice([
        "img/hombre-1.png", 
        "img/mujeres-2.png",
        "img/caballodorado.png",
        "img/grupo.png",
        "img/lloron.png"
        ])

# Crear un desplegable para seleccionar la ruta específica
selected_viaje = st.sidebar.selectbox("Selecciona el viaje:", ["General"] + viaje_ids.tolist())

if selected_viaje == "General":
    # Crear un mapa con Folium
    m = folium.Map(location=[data_personas["latitud"].mean(), data_personas["longitud"].mean()], zoom_start=15)

    # Obtener colores aleatorios para cada ruta
    random.seed(42)  # Para obtener los mismos colores cada vez que se ejecute
    colores_ruta = {}
    for viaje_id in viaje_ids:
        colores_ruta[viaje_id] = "#{:06x}".format(random.randint(0, 0xFFFFFF))  

    # Agregar marcadores al inicio y final de cada ruta con una línea que conecta los puntos intermedios
    for viaje_id in viaje_ids:
        selected_data_viaje = data_viajes[data_viajes["viaje_id"] == viaje_id]
        inicio_viaje = [selected_data_viaje.iloc[0]["latitud"], selected_data_viaje.iloc[0]["longitud"]]
        final_viaje = [selected_data_viaje.iloc[-1]["latitud"], selected_data_viaje.iloc[-1]["longitud"]]
        intermediate_points_viaje = selected_data_viaje[["latitud", "longitud"]].values.tolist()

        # Agregar marcadores de inicio y final
        icon_path_inicio = 'img/inicio.png'
        icon_path_final = 'img/final.png'

        icon_inicio = folium.CustomIcon(icon_image=icon_path_inicio, icon_size=(55, 55))
        icon_final = folium.CustomIcon(icon_image=icon_path_final, icon_size=(55, 55))
        
        folium.Marker(location=inicio_viaje, icon=icon_inicio, popup=f"Inicio Viaje {viaje_id}").add_to(m)
        folium.Marker(location=final_viaje, icon=icon_final, popup=f"Final Viaje {viaje_id}").add_to(m)

        # Agregar línea que conecta los puntos intermedios
        folium.PolyLine(locations=intermediate_points_viaje, color=colores_ruta[viaje_id], weight=2).add_to(m)

    # Agregar marcadores para las personas en el mapa general
    for index, row in data_personas.iterrows():
        icono_persona_file = imagenes_predeterminadas[(row['latitud'], row['longitud'])]
        icono_persona = folium.CustomIcon(icon_image=icono_persona_file, icon_size=(25, 25))
        folium.Marker([row['latitud'], row['longitud']], icon=icono_persona).add_to(m)

    # Mostrar el mapa con Streamlit
    st.title("Mapa General")
    folium_static(m)

else:
    # Crear un mapa que muestre la ruta seleccionada
    m_viaje = folium.Map(location=[data_viajes["latitud"].mean(), data_viajes["longitud"].mean()], zoom_start=10)

    # Filtrar los datos por el viaje seleccionado
    selected_data_viaje = data_viajes[data_viajes["viaje_id"] == selected_viaje]

    # Obtener un color aleatorio para la ruta seleccionada
    random.seed(42)  # Para obtener los mismos colores cada vez que se ejecute
    color_viaje = "#{:06x}".format(random.randint(0, 0xFFFFFF))  

    # Agregar marcador al inicio de la ruta
    inicio_viaje = [selected_data_viaje.iloc[0]["latitud"], selected_data_viaje.iloc[0]["longitud"]]
    folium.Marker(location=inicio_viaje, icon=folium.Icon(color="green"), popup=f"Inicio Viaje {selected_viaje}").add_to(m_viaje)

    # Agregar marcador al final de la ruta
    final_viaje = [selected_data_viaje.iloc[-1]["latitud"], selected_data_viaje.iloc[-1]["longitud"]]
    folium.Marker(location=final_viaje, icon=folium.Icon(color="red"), popup=f"Final Viaje {selected_viaje}").add_to(m_viaje)

    # Crear una línea que conecte los puntos intermedios de la ruta
    intermediate_points_viaje = selected_data_viaje[["latitud", "longitud"]].values.tolist()
    folium.PolyLine(locations=intermediate_points_viaje, color=color_viaje, weight=2).add_to(m_viaje)

    # Agregar marcadores para cada punto de las personas a recoger en la ruta seleccionada
    for index, row in data_personas.iterrows():
        if row['viaje_id'] == selected_viaje:
            # Obtener la imagen predeterminada de la persona
            icono_persona_file = imagenes_predeterminadas[(row['latitud'], row['longitud'])]
            icono_persona = folium.CustomIcon(icon_image=icono_persona_file, icon_size=(32, 32))
            folium.Marker([row['latitud'], row['longitud']], icon=icono_persona).add_to(m_viaje)

    # Agregar marcador del coche en la posición inicial
    icono_coche = folium.CustomIcon(icon_image="img/coche-toro.png", icon_size=(32, 32))
    marker_coche = folium.Marker(location=inicio_viaje, icon=icono_coche)
    marker_coche.add_to(m_viaje)

    # Función para actualizar la ubicación del coche cuando se presiona el botón
    def actualizar_coordenadas_y_mover_coche():
        for i in range(0, len(intermediate_points_viaje), 20):  # Avanza de 20 en 20
            marker_coche.location = [intermediate_points_viaje[i][0], intermediate_points_viaje[i][1]]
            folium_static(m_viaje)
            time.sleep(1)
            

    # Mostrar el mapa de la ruta seleccionada
    st.title(f"Mapa de la Ruta {selected_viaje}")
    folium_static(m_viaje)

    # Botón para actualizar las coordenadas de la ruta y mover el "coche"
    if st.button("Actualizar Coordenadas y Mover Coche"):
        actualizar_coordenadas_y_mover_coche()