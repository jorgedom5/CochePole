import streamlit as st
import pandas as pd
import folium
import random
import time
from streamlit_folium import folium_static
from google.cloud import bigquery

client = bigquery.Client(project='dataproject-2-413010')

query_viajes = "SELECT * FROM dataproject-2-413010.BBDD.tabla_viajes_1"
query_personas = "SELECT latitud, longitud, viaje_id FROM dataproject-2-413010.BBDD.CochePole_BD"

@st.cache_data
def load_data():
    return client.query(query_viajes).to_dataframe(), client.query(query_personas).to_dataframe()

data_viajes, data_personas = load_data()
data_personas_sampled = data_personas.sample(frac=0.1, random_state=42)
viaje_ids = data_viajes["viaje_id"].unique()

if st.sidebar.selectbox("Selecciona el viaje:", ["General"] + viaje_ids.tolist()) == "General":
    m = folium.Map(location=[data_personas_sampled["latitud"].mean(), data_personas_sampled["longitud"].mean()], zoom_start=15)
    colores_ruta = {viaje_id: "#{:06x}".format(random.randint(0, 0xFFFFFF)) for viaje_id in viaje_ids}
    
    for viaje_id in viaje_ids:
        selected_data_viaje = data_viajes[data_viajes["viaje_id"] == viaje_id]
        intermediate_points_viaje = selected_data_viaje[["latitud", "longitud"]].values.tolist()

        folium.Marker(location=intermediate_points_viaje[0], icon=folium.CustomIcon(icon_image='img/inicio.png', icon_size=(55, 55))).add_to(m)
        folium.Marker(location=intermediate_points_viaje[-1], icon=folium.CustomIcon(icon_image='img/final.png', icon_size=(55, 55))).add_to(m)

        folium.PolyLine(locations=intermediate_points_viaje, color=colores_ruta[viaje_id], weight=2).add_to(m)

    for _, row in data_personas_sampled.iterrows():
        icono_persona_file = random.choice(["img/hombre-1.png", "img/mujeres-2.png", "img/caballodorado.png", "img/grupo.png", "img/lloron.png"])
        icono_persona = folium.CustomIcon(icon_image=icono_persona_file, icon_size=(25, 25))
        folium.Marker([row['latitud'], row['longitud']], icon=icono_persona).add_to(m)

    st.title("Mapa General")
    folium_static(m)

else:
    selected_viaje = st.sidebar.selectbox("Selecciona el viaje:", viaje_ids.tolist())
    m_viaje = folium.Map(location=[data_viajes["latitud"].mean(), data_viajes["longitud"].mean()], zoom_start=10)

    selected_data_viaje = data_viajes[data_viajes["viaje_id"] == selected_viaje]
    color_viaje = "#{:06x}".format(random.randint(0, 0xFFFFFF))

    folium.Marker(location=selected_data_viaje.iloc[0][["latitud", "longitud"]], icon=folium.Icon(color="green"), popup=f"Inicio Viaje {selected_viaje}").add_to(m_viaje)
    folium.Marker(location=selected_data_viaje.iloc[-1][["latitud", "longitud"]], icon=folium.Icon(color="red"), popup=f"Final Viaje {selected_viaje}").add_to(m_viaje)

    folium.PolyLine(locations=selected_data_viaje[["latitud", "longitud"]].values.tolist(), color=color_viaje, weight=2).add_to(m_viaje)

    for _, row in data_personas_sampled[data_personas_sampled['viaje_id'] == selected_viaje].iterrows():
        icono_persona_file = random.choice(["img/hombre-1.png", "img/mujeres-2.png", "img/caballodorado.png", "img/grupo.png", "img/lloron.png"])
        icono_persona = folium.CustomIcon(icon_image=icono_persona_file, icon_size=(40, 40))
        folium.Marker([row['latitud'], row['longitud']], icon=icono_persona).add_to(m_viaje)

    icono_coche = folium.CustomIcon(icon_image="img/coche-toro.png", icon_size=(32, 32))
    marker_coche = folium.Marker(location=selected_data_viaje.iloc[0][["latitud", "longitud"]], icon=icono_coche)
    marker_coche.add_to(m_viaje)

    def actualizar_coordenadas_y_mover_coche():
        for i in range(0, len(selected_data_viaje), 20):
            marker_coche.location = [selected_data_viaje.iloc[i]["latitud"], selected_data_viaje.iloc[i]["longitud"]]
            folium_static(m_viaje)
            time.sleep(1)

    st.title(f"Mapa de la Ruta {selected_viaje}")
    folium_static(m_viaje)

    if st.button("Actualizar Coordenadas y Mover Coche"):
        actualizar_coordenadas_y_mover_coche()
