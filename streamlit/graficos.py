import streamlit as st
import folium
from folium.plugins import HeatMap
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go

# Crear cliente de BigQuery
client = bigquery.Client(project='dataproject-2-413010')

## GRÁFICO DE RECAUDACIÓN POR CONDUCTOR

query_df = """
SELECT p.*, 
v.nombre_conductor, v.marca_coche, v.color_coche, v.anio_fabricacion, 
v.tipo_combustible, v.tiene_calefaccion, v.puntos_carnet, v.cilindraje_motor, v.kilometraje,
c.nombre_cliente, c.apellido_cliente, c.edad_cliente, c.genero_cliente,
FROM dataproject-2-413010.BBDD.CochePole_BD p
left join dataproject-2-413010.BBDD.tabla_vehiculos v on p.vehiculo_id = v.vehiculo_id
left join dataproject-2-413010.BBDD.tabla_clientes c on p.cliente_id = c.cliente_id
"""

df = client.query(query_df).to_dataframe()

df

# GRÁFICO DE RECAUDACIÓN POR CONDUCTOR
recaudacion_df = df.groupby('nombre_conductor')['pago_viaje'].sum().reset_index()

st.title('Recaudación por Conductor al Final del Día')
fig_recaudacion = px.bar(recaudacion_df, x='nombre_conductor', y='pago_viaje', labels={'pago_viaje': 'Recaudación'})
st.plotly_chart(fig_recaudacion)

# GRÁFICO DE Nº VIAJES POR CLIENTE
viajes_por_cliente_df = df['cliente_id'].value_counts().reset_index()
viajes_por_cliente_df.columns = ['cliente_id', 'num_viajes']

st.title('Número de Viajes Realizados por Cliente')
fig_viajes_por_cliente = px.bar(viajes_por_cliente_df, x='num_viajes', y='cliente_id', labels={'num_viajes': 'Número de Viajes', 'cliente_id': 'ID del Cliente'})
st.plotly_chart(fig_viajes_por_cliente)

# GRÁFICO DE Nº VIAJES POR CONDUCTOR
viajes_por_conductor_df = df['vehiculo_id'].value_counts().reset_index()
viajes_por_conductor_df.columns = ['vehiculo_id', 'num_viajes']

st.title('Número de Viajes Realizados por Conductor')
fig_viajes_por_conductor = px.bar(viajes_por_conductor_df, x='num_viajes', y='vehiculo_id', labels={'num_viajes': 'Número de Viajes', 'vehiculo_id': 'ID del Conductor'})
st.plotly_chart(fig_viajes_por_conductor)

# GRÁFICO METODO DE PAGO MÁS UTILIZADO
metodo_pago_preferido_df = df['metodo_pago'].value_counts().reset_index()
metodo_pago_preferido_df.columns = ['metodo_pago', 'cantidad']
metodo_pago_preferido_df['porcentaje'] = metodo_pago_preferido_df['cantidad'] * 100.0 / len(df)

st.title('Método de pago preferido por los clientes')
fig_metodo_pago = px.pie(metodo_pago_preferido_df, names='metodo_pago', values='cantidad', labels={'cantidad': 'Cantidad'}, title='Método de Pago Preferido')
st.plotly_chart(fig_metodo_pago)

rating_medio_por_vehiculo_df = df.groupby('vehiculo_id')['rating'].mean().reset_index()
rating_medio_por_vehiculo_df.columns = ['vehiculo_id', 'puntuacion_promedio']

# GRÁFICO DE NOTA PROMEDIA POR VEHÍCULO
fig_rating_promedio = px.histogram(rating_medio_por_vehiculo_df, x='puntuacion_promedio', nbins=7,
                                   labels={'puntuacion_promedio': 'Rating Promedio'},
                                   title='Histograma de Ratings Promedio por Vehículo')

fig_rating_promedio.update_xaxes(range=[1, 10])

fig_rating_promedio.update_traces(marker_color='skyblue', marker_line_color='black', marker_line_width=1)

st.title('Nota Promedia de Nuestros Clientes')
st.plotly_chart(fig_rating_promedio)

# ## GRÁFICO MAPA DE CALOR DE COORDENADAS

fig = px.density_mapbox(
    df,
    lat='latitud',
    lon='longitud',
    radius=30, # CAMBIAR TAMAÑO CIRCULOS
    center=dict(lat=df['latitud'].mean(), lon=df['longitud'].mean()),
    color_continuous_scale="inferno",
    zoom=12,
    mapbox_style="open-street-map", 
    title='Mapa de Densidad de Coordenadas de Viajes'
)

st.plotly_chart(fig)