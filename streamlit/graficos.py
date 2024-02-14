import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery

# Crear cliente de BigQuery
client = bigquery.Client(project='dataproject-2-413010')

## GRÁFICO DE RECAUDACIÓN POR CONDUCTOR

# Consulta SQL para calcular la recaudación por conductor
query_recaudacion = """
SELECT vehicle_id, SUM(recaudacion) AS total_recaudado
FROM `dataproject-2-413010.BBDD.---nombretablarecaudacion---`
GROUP BY vehicle_id
"""

# Ejecutar la consulta y obtener los resultados
recaudacion_df = client.query(query_recaudacion).to_dataframe()

# Visualización en Streamlit
st.title('Recaudación por Conductor al Final del Día')
st.bar_chart(recaudacion_df.set_index('vehicle_id'))

## GRÁFICO DE Nº VIAJES POR CLIENTE

# Consulta SQL para contar el número de viajes por cliente
query_viajes_por_cliente = """
SELECT cliente_id, COUNT(*) AS num_viajes
FROM `dataproject-2-413010.BBDD.--nombretabla--`
GROUP BY user_id
"""

# Ejecutar la consulta y obtener los resultados
viajes_por_cliente_df = client.query(query_viajes_por_cliente).to_dataframe()

# Visualización en Streamlit
st.title('Número de Viajes Realizados por Cliente')
st.bar_chart(viajes_por_cliente_df.set_index('user_id'))

## GRÁFICO DE Nº VIAJES POR CONDUCTOR

# Consulta SQL para contar el número de viajes por conductor
query_viajes_por_conductor = """
SELECT vehicle_id, COUNT(*) AS num_viajes
FROM `dataproject-2-413010.BBDD.--TABLA--`
GROUP BY vehicle_id
"""

# Ejecutar la consulta y obtener los resultados
viajes_por_conductor_df = client.query(query_viajes_por_conductor).to_dataframe()

# Visualización en Streamlit
st.title('Número de Viajes Realizados por Conductor')
st.bar_chart(viajes_por_conductor_df.set_index('vehicle_id'))

# También puedes mostrar los datos en forma de tabla si lo deseas
st.subheader('Detalle de Viajes por Conductor')
st.write(viajes_por_conductor_df)

