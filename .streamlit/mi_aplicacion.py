import streamlit as st
from streamlit_folium import folium_static
import folium

# Coordenadas de inicio y fin de la ruta (ejemplo)
start_coords = [37.774929, -122.419418]
end_coords = [37.775154, -122.419665]

# Crear el mapa
mymap = folium.Map(location=start_coords, zoom_start=15)

# Añadir marcadores para el inicio y fin de la ruta
folium.Marker(location=start_coords, popup='Inicio').add_to(mymap)
folium.Marker(location=end_coords, popup='Fin').add_to(mymap)

# Añadir la ruta
folium.PolyLine(locations=[start_coords, end_coords], color='blue').add_to(mymap)

# Mostrar el mapa en Streamlit
folium_static(mymap)


