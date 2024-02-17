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

## QUERY

query_df = """
SELECT p.*, 
v.nombre_conductor, v.marca_coche, v.color_coche, v.anio_fabricacion, 
v.tipo_combustible, v.tiene_calefaccion, v.puntos_carnet, v.cilindraje_motor, v.kilometraje, v.anio_registro_app,
c.nombre_cliente, c.apellido_cliente, c.edad_cliente, c.genero_cliente,
FROM dataproject-2-413010.BBDD.CochePole_BD p
left join dataproject-2-413010.BBDD.tabla_vehiculos v on p.vehiculo_id = v.vehiculo_id
left join dataproject-2-413010.BBDD.tabla_clientes c on p.cliente_id = c.cliente_id
"""

df = client.query(query_df).to_dataframe()

# A BORRAR
df

df.dtypes


# SECCIÓN KPI

# KPI 1: Recaudación total
recaudacion_total = df['pago_viaje'].sum()
st.sidebar.metric("Recaudación Total", f"{recaudacion_total:,.2f}€", delta=recaudacion_total)

# KPI 2: Número total de viajes
total_viajes = df.groupby(['vehiculo_id', 'viaje_id']).size().reset_index(name='count').shape[0]
st.sidebar.metric("Número Total de Viajes", total_viajes)

# KPI 3: Recaudación promedio por viaje
recaudacion_promedio_por_viaje = recaudacion_total / total_viajes
st.sidebar.metric("Recaudación Promedio por Viaje", f"{recaudacion_promedio_por_viaje:,.2f}€", delta=recaudacion_promedio_por_viaje)

# KPI 4: Número de conductores activos
conductores_activos = df['nombre_conductor'].nunique()
st.sidebar.metric("Número de Conductores Activos", conductores_activos)

# KPI 5: Número de clientes únicos
clientes_unicos = df['cliente_id'].nunique()
st.sidebar.metric("Número de Clientes Únicos", clientes_unicos)

# KPI 6: Promedio de rating de clientes
rating_promedio_conductores = df.groupby('nombre_conductor')['rating'].mean().mean()
st.sidebar.metric("Rating Promedio de Clientes", round(rating_promedio_conductores, 2), delta=rating_promedio_conductores)

# KPI 7: Promedio de edad de clientes
edad_media_clientes = df['edad_cliente'].mean()
st.sidebar.metric("Edad Media de los Clientes", round(edad_media_clientes, 2), delta=edad_media_clientes)

# KPI 8: Kilometraje promedio por viaje
kilometraje_promedio_por_viaje = df['anio_registro_app'].mean()
st.sidebar.metric("Año Promedio Registro", f"{kilometraje_promedio_por_viaje:,.0f}")


# SECCIÓN GRAFICOS

# GRÁFICO DE RECAUDACIÓN POR CONDUCTOR
recaudacion_df = df.groupby('nombre_conductor')['pago_viaje'].sum().reset_index()
recaudacion_df = recaudacion_df.sort_values(by='pago_viaje', ascending=False).head(14)

st.title('Top 14 Ganancias por conductor')
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


# GRÁFICO RECAUDACIÓN COCHE
recaudacion_por_marca_df = df.groupby('marca_coche')['pago_viaje'].sum().reset_index()
recaudacion_por_marca_df = recaudacion_por_marca_df.sort_values(by='pago_viaje', ascending=False).head(14)

st.title('Top 14 Recaudación por Marca de Coche')
fig_recaudacion_por_marca = px.bar(recaudacion_por_marca_df, x='marca_coche', y='pago_viaje', 
                                   labels={'pago_viaje': 'Recaudación por Marca de Coche'},
                                   title='Top 14 Recaudación por Marca de Coche')
st.plotly_chart(fig_recaudacion_por_marca)


# GRÁFICO DE DISPERSIÓN ENTRE RATING Y RECAUDACIÓN POR VIAJE
fig_scatter_3d = px.scatter_3d(df, x='pago_viaje', y='rating', z='edad_cliente',
                               color='genero_cliente', size='pago_viaje',
                               labels={'recaudacion_total': 'Recaudación Total', 'rating': 'Rating', 'edad_cliente': 'Edad del Cliente'},
                               title='Relación Tridimensional entre Recaudación, Rating y Edad del Cliente')
st.plotly_chart(fig_scatter_3d)

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