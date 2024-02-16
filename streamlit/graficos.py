import streamlit as st
import folium
from folium.plugins import HeatMap
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery

# Crear cliente de BigQuery
client = bigquery.Client(project='dataproject-2-413010')

## GRÁFICO DE RECAUDACIÓN POR CONDUCTOR

# Consulta SQL para calcular la recaudación por conductor
query_recaudacion = """
SELECT vehiculo_id, SUM(pago_viaje) AS total_recaudado
FROM `dataproject-2-413010.BBDD.simulador_streamlit2`
GROUP BY vehiculo_id
"""

# Ejecutar la consulta y obtener los resultados
recaudacion_df = client.query(query_recaudacion).to_dataframe()

# Visualización en Streamlit
st.title('Recaudación por Conductor al Final del Día')
st.bar_chart(recaudacion_df.set_index('vehiculo_id'))

## GRÁFICO DE Nº VIAJES POR CLIENTE

# Consulta SQL para contar el número de viajes por cliente
query_viajes_por_cliente = """
SELECT cliente_id, COUNT(*) AS num_viajes
FROM `dataproject-2-413010.BBDD.simulador_streamlit2`
GROUP BY cliente_id
"""

# Ejecutar la consulta y obtener los resultados
viajes_por_cliente_df = client.query(query_viajes_por_cliente).to_dataframe()

# Visualización en Streamlit
st.title('Número de Viajes Realizados por Cliente')
st.bar_chart(viajes_por_cliente_df.set_index('cliente_id'))

## GRÁFICO DE Nº VIAJES POR CONDUCTOR

# Consulta SQL para contar el número de viajes por conductor
query_viajes_por_conductor = """
SELECT vehiculo_id, COUNT(*) AS num_viajes
FROM `dataproject-2-413010.BBDD.simulador_streamlit2`
GROUP BY vehiculo_id
"""

# Ejecutar la consulta y obtener los resultados
viajes_por_conductor_df = client.query(query_viajes_por_conductor).to_dataframe()

# Visualización en Streamlit
st.title('Número de Viajes Realizados por Conductor')
st.bar_chart(viajes_por_conductor_df.set_index('vehiculo_id'))

# También puedes mostrar los datos en forma de tabla si lo deseas
st.subheader('Detalle de Viajes por Conductor')
st.write(viajes_por_conductor_df)

## GRÁFICO METODO DE PAGO MÁS UTILIZADO

# Query Método Pago

query_metodo_pago = """
SELECT metodo_pago as metodo_pago
FROM `dataproject-2-413010.BBDD.simulador_streamlit2`
GROUP BY metodo_pago
"""

metodo_pago_preferido_df = client.query(query_metodo_pago).to_dataframe()

st.title('Método de pago preferido por los clientes')

# Gráfico de pastel
fig, ax = plt.subplots()
metodo_pago_preferido_df['metodo_pago'].value_counts().plot(kind='pie', autopct='%1.1f%%', ax=ax)
ax.set_ylabel('')  # Elimina la etiqueta del eje y

st.pyplot(fig)

# Query Rating
query_rating_medio_por_vehiculo = """
SELECT vehiculo_id, AVG(rating) as puntuacion_promedio
FROM `dataproject-2-413010.BBDD.simulador_streamlit2`
GROUP BY vehiculo_id
"""

rating_medio_por_vehiculo_df = client.query(query_rating_medio_por_vehiculo).to_dataframe()

# Título del Gráfico
st.title('Histograma de Ratings Promedio por Vehículo')

# Histograma de los ratings promedio por vehículo
fig, ax = plt.subplots()
ax.hist(rating_medio_por_vehiculo_df['puntuacion_promedio'], bins=5, color='skyblue', edgecolor='black')
ax.set_xlabel('Rating Promedio')
ax.set_ylabel('Frecuencia')
ax.set_title('Histograma de Ratings Promedio por Vehículo')
st.pyplot(fig)

# ## GRÁFICO MAPA DE CALOR DE COORDENADAS

query_coordenadas_viaje = """
SELECT latitud, longitud
FROM `dataproject-2-413010.BBDD.simulador_streamlit2`
"""

# Ejecutar la consulta y obtener los resultados
resultados = client.query(query_coordenadas_viaje).result()

# Crear un mapa centrado en un punto promedio de las coordenadas
m = folium.Map(location=[0, 0], zoom_start=2)

# Agrupar las coordenadas de los viajes en una lista
heat_data = [[row['latitud'], row['longitud']] for row in resultados]

# Agregar el mapa de calor al mapa
HeatMap(heat_data).add_to(m)

# Guardar el mapa como HTML en una ruta específica
html_file_path = "heatmap.html"
m.save(html_file_path)

# Mostrar el mapa en Streamlit usando un iframe
st.write('<iframe src="{}" width="100%" height="500"></iframe>'.format(html_file_path), unsafe_allow_html=True)