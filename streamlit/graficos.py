import streamlit as st
import folium
from folium.plugins import HeatMap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
import joblib
from datetime import datetime

best_model = joblib.load('../machine_learning/modelo_final.joblib')

# Crear cliente de BigQuery
client = bigquery.Client(project='dataproject-2-413010')


st.set_page_config(
    page_title="BLABLACAR MARVELOUS STREAMLIT",
    page_icon="img/logo.png"
)


## QUERY

query_df = """
SELECT p.*, 
v.nombre_conductor, v.marca_coche, v.color_coche, v.anio_fabricacion, 
v.tipo_combustible, v.tiene_calefaccion, v.puntos_carnet, v.cilindraje_motor, v.kilometraje, v.anio_registro_app,
c.nombre_cliente, c.apellido_cliente, c.edad_cliente, c.genero_cliente,
c.direccion_cliente, c.trabajo_cliente, c.fecha_registro,
FROM dataproject-2-413010.BBDD.CochePole_BD p
left join dataproject-2-413010.BBDD.tabla_vehiculos v on p.vehiculo_id = v.vehiculo_id
left join dataproject-2-413010.BBDD.tabla_clientes c on p.cliente_id = c.cliente_id
"""

df = client.query(query_df).to_dataframe()

logo_path = 'img/logo.png'  # Cambia la ruta a la ubicación real de tu logo
logo_image = st.image(logo_path, use_column_width=True)

# botones
selected_page = st.radio("Seleccione una sección:", ["Análisis Visual", "Predicción ML"])


if selected_page == "Análisis Visual":
    st.header("Análisis visual")

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
    
    # VEHÍCULOS
    
    st.subheader("VEHÍCULOS")

    # GRÁFICO DE RECAUDACIÓN POR CONDUCTOR
    recaudacion_df = df.groupby('nombre_conductor')['pago_viaje'].sum().reset_index()
    recaudacion_df = recaudacion_df.sort_values(by='pago_viaje', ascending=False).head(14)

    st.title('Top 14 Ganancias por conductor')
    fig_recaudacion = px.bar(recaudacion_df, x='nombre_conductor', y='pago_viaje', labels={'pago_viaje': 'Recaudación'})
    st.plotly_chart(fig_recaudacion)
    
    # GRÁFICO DE Nº VIAJES POR CONDUCTOR
    viajes_por_conductor_df = df['vehiculo_id'].value_counts().reset_index()
    viajes_por_conductor_df.columns = ['vehiculo_id', 'num_viajes']

    st.title('Número de Viajes Realizados por Conductor')
    fig_viajes_por_conductor = px.bar(viajes_por_conductor_df, x='num_viajes', y='vehiculo_id', labels={'num_viajes': 'Número de Viajes', 'vehiculo_id': 'ID del Conductor'})
    st.plotly_chart(fig_viajes_por_conductor)
    
    # GRÁFICO RECAUDACIÓN MARCA
    recaudacion_por_marca_df = df.groupby('marca_coche')['pago_viaje'].sum().reset_index()
    recaudacion_por_marca_df = recaudacion_por_marca_df.sort_values(by='pago_viaje', ascending=False).head(14)

    st.title('Top 14 Recaudación por Marca de Coche')
    fig_recaudacion_por_marca = px.bar(recaudacion_por_marca_df, x='marca_coche', y='pago_viaje', 
                                    labels={'pago_viaje': 'Recaudación por Marca de Coche'},
                                    title='Top 14 Recaudación por Marca de Coche')
    st.plotly_chart(fig_recaudacion_por_marca)

    # GRÁFICO RECAUDACIÓN AÑO DEL COCHE
    recaudacion_por_anio_df = df.groupby('anio_fabricacion')['pago_viaje'].sum().reset_index()
    recaudacion_por_anio_df = recaudacion_por_anio_df.sort_values(by='pago_viaje', ascending=False)

    fig_recaudacion_por_anio = px.bar(recaudacion_por_anio_df, x='anio_fabricacion', y='pago_viaje',
                                    labels={'anio_fabricacion': 'Año de Fabricación del Coche', 'pago_viaje': 'Recaudación Total'},
                                    title='Recaudación por Año de Fabricación del Coche'
                                    )

    st.plotly_chart(fig_recaudacion_por_anio)
    
    # GRÁFICO DE BOX PLOT: RELACIÓN ENTRE RATING Y COLOR DE COCHE
    top_colores = df['color_coche'].value_counts().head(14).index.tolist()

    df_top_colores = df[df['color_coche'].isin(top_colores)]

    fig_boxplot_rating_color = px.box(df_top_colores, x='color_coche', y='rating',
                                    color='color_coche',
                                    labels={'rating': 'Rating', 'color_coche': 'Color de Coche'},
                                    title='Relación entre Rating de Clientes y Color de Coche')

    st.title('Box Plot: Relación entre Rating de Clientes y Top 14 Colores de Coche')
    st.plotly_chart(fig_boxplot_rating_color)


    # GRÁFICO DE RADAR: PERFIL PROMEDIO DEL CONDUCTOR
    perfil_conductor_df = df.groupby('nombre_conductor').agg({
        'rating': 'mean',
        'kilometraje': 'mean',
        'edad_cliente': 'mean',
        'puntos_carnet': 'mean'
    }).reset_index()

    for col in ['rating', 'kilometraje', 'edad_cliente', 'puntos_carnet']:
        perfil_conductor_df[col] = (perfil_conductor_df[col] - perfil_conductor_df[col].min()) / (perfil_conductor_df[col].max() - perfil_conductor_df[col].min())

    fig_radar = go.Figure()

    fig_radar.add_trace(go.Scatterpolar(
        r=perfil_conductor_df.loc[0, ['rating', 'kilometraje', 'edad_cliente', 'puntos_carnet']],
        theta=['Rating', 'Kilometraje', 'Edad', 'Puntos de Carnet'],
        fill='toself',
        name=perfil_conductor_df.loc[0, 'nombre_conductor']
    ))

    fig_radar.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1]
            )),
        showlegend=True,
        title='Perfil Promedio del Conductor'
    )
    
    # GRÁFICO DE LOS TIPOS DE VEHÍCULOS UTILIZADOS

    coches_por_combustible = df['tipo_combustible'].value_counts()

    paleta_colores = {
        'Gasolina': '#FF5733', 
        'Híbrido': '#45B6AF',  
        'Eléctrico': '#488AC7' 
    }

    fig2 = px.pie(names=coches_por_combustible.index, 
                values=coches_por_combustible.values, 
                title='Distribución de Coches por Tipo de Combustible',
                color=coches_por_combustible.index,
                color_discrete_map=paleta_colores)

    st.plotly_chart(fig2)

    # GRÁFICO VIOLIN DE CILINDRADA DE MOTOR
    fig = go.Figure(data=go.Violin(y=df['cilindraje_motor'], box_visible=True, line_color='black', meanline_visible=True,
                                fillcolor='lightseagreen', opacity=0.6))

    fig.update_layout(
        title='Distribución de la Cilindrada del Motor de los coches',
        xaxis=dict(title='Cilindrada del Motor (en miles)'),
        yaxis=dict(title=''),
        xaxis_tickformat=',d'
    )

    st.plotly_chart(fig)
    
    
    
    
    # CLIENTES
    
    st.subheader("CLIENTES")

    # GRÁFICO DE Nº VIAJES POR CLIENTE
    viajes_por_cliente_df = df['cliente_id'].value_counts().reset_index()
    viajes_por_cliente_df.columns = ['cliente_id', 'num_viajes']

    st.title('Número de Viajes Realizados por Cliente')
    fig_viajes_por_cliente = px.bar(viajes_por_cliente_df, x='num_viajes', y='cliente_id', labels={'num_viajes': 'Número de Viajes', 'cliente_id': 'ID del Cliente'})
    st.plotly_chart(fig_viajes_por_cliente)

    # ## GRÁFICO MAPA DE CALOR DE COORDENADAS

    fig = px.density_mapbox(
        df,
        lat='latitud',
        lon='longitud',
        radius=25, # CAMBIAR TAMAÑO CIRCULOS
        center=dict(lat=df['latitud'].mean(), lon=df['longitud'].mean()),
        color_continuous_scale="inferno",
        zoom=12,
        mapbox_style="open-street-map", 
        title='Mapa de Densidad de Coordenadas de Viajes -- Recogidas'
    )

    st.plotly_chart(fig)
    
    # ## GRÁFICO MAPA DE CALOR DE COORDENADAS 2
    
    fig_heatmap_final = px.density_mapbox(
    df,
    lat='latitud_final',
    lon='longitud_final',
    radius=25, 
    center=dict(lat=df['latitud_final'].mean(), lon=df['longitud_final'].mean()),
    color_continuous_scale="inferno",
    zoom=12,
    mapbox_style="open-street-map", 
    title='Mapa de Densidad de Coordenadas de Viajes -- Destinos'
    )

    st.plotly_chart(fig_heatmap_final)

    # GRÁFICO METODO DE PAGO MÁS UTILIZADO
    metodo_pago_preferido_df = df['metodo_pago'].value_counts().reset_index()
    metodo_pago_preferido_df.columns = ['metodo_pago', 'cantidad']
    metodo_pago_preferido_df['porcentaje'] = metodo_pago_preferido_df['cantidad'] * 100.0 / len(df)

    st.title('Método de pago preferido por los clientes')
    fig_metodo_pago = px.pie(metodo_pago_preferido_df, names='metodo_pago', values='cantidad', labels={'cantidad': 'Cantidad'}, title='Método de Pago Preferido')
    st.plotly_chart(fig_metodo_pago)

    rating_medio_por_vehiculo_df = df.groupby('vehiculo_id')['rating'].mean().reset_index()
    rating_medio_por_vehiculo_df.columns = ['vehiculo_id', 'puntuacion_promedio']

    # GRÁFICO DE NOTA PROMEDIA POR CLIENTE
    fig_rating_promedio = px.histogram(rating_medio_por_vehiculo_df, x='puntuacion_promedio', nbins=7,
                                    labels={'puntuacion_promedio': 'Rating Promedio'},
                                    title='Histograma de Ratings Promedio por Cliente')

    fig_rating_promedio.update_xaxes(range=[1, 10])

    fig_rating_promedio.update_traces(marker_color='skyblue', marker_line_color='black', marker_line_width=1)

    st.title('Nota Promedia de Nuestros Clientes')
    st.plotly_chart(fig_rating_promedio)


    # GRÁFICO DE CAMPANA DE PUNTOS DE CARNET DE CONDUCIR

    mu, sigma = df['puntos_carnet'].mean(), df['puntos_carnet'].std()
    x = np.linspace(mu - 3*sigma, mu + 3*sigma, 100)
    gaussiana = (1/(sigma * np.sqrt(2 * np.pi))) * np.exp(-0.5 * ((x - mu) / sigma) ** 2)

    histograma = go.Histogram(x=df['puntos_carnet'], histnorm='probability density', name='Histograma')

    campana_gauss = go.Scatter(x=x, y=gaussiana, mode='lines', name='Campana de Gauss', line=dict(color='red', width=2))

    fig = go.Figure(data=[histograma, campana_gauss])

    fig.update_layout(
        title='Distribución de Puntos del Carnet de Conducir (Histograma y Campana de Gauss)',
        xaxis=dict(title='Puntos del Carnet de Conducir', range=[1, 15]),  # Set the x-axis range
        yaxis=dict(title='Densidad'),
    )

    st.plotly_chart(fig)
    
    # HISTOGRAMA EDAD
    
    fig_edad_cliente = px.histogram(df, x='edad_cliente', nbins=20,
                                labels={'edad_cliente': 'Edad del Cliente'},
                                title='Histograma de Edad de Clientes')
    st.plotly_chart(fig_edad_cliente)

    
    
    



elif selected_page == "Predicción ML":
    st.header("Predicción Ganancias -- Machine Learning")

    rating = st.slider('Rating del cliente', 0.0, 10.0, 8.0)
    metodo_pago = st.selectbox('Método de pago', ['Efectivo', 'Tarjeta', 'Bizum'])
    viaje_id = st.number_input('Viaje ID', value=12)
    latitud_final = st.number_input('Latitud del destino del viaje', value=39.47947)
    longitud_final = st.number_input('Longitud del destino del viaje', value=-0.37594)
    timestamp_str = st.text_input('Fecha y hora del viaje (YYYY-MM-DD HH:MM:SS)', '2024-02-17 11:10:33')
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    marca_coche = st.text_input('Marca del coche', 'BMW')
    color_coche = st.text_input('Color del coche', 'Marrón siena')
    anio_fabricacion = st.number_input('Año de fabricación del coche', value=2020)
    tipo_combustible = st.selectbox('Tipo de combustible', ['Gasolina', 'Híbrido', 'Eléctrico'])
    tiene_calefaccion = st.selectbox('Tiene calefacción', [0, 1])
    puntos_carnet = st.number_input('Puntos del carnet de conducir', value=11)
    cilindraje_motor = st.number_input('Cilindraje del motor', value=2500)
    kilometraje = st.number_input('Kilometraje del coche', value=42353)
    anio_registro_app = st.number_input('Año de registro en la aplicación', value=2022)
    edad_cliente = st.number_input('Edad del cliente', value=48)
    genero_cliente = st.selectbox('Género del cliente', ['masculino', 'femenino', 'desconocido'])
    trabajo_cliente = st.text_input('Trabajo del cliente', 'Parquetero y colocador de suelos')
    fecha_registro = st.text_input('Fecha de registro en la aplicación (YYYY-MM-DD)', '2023-09-24')

    if st.button('Realizar Predicción'):
        cliente_data = {
            'rating': [rating],
            'metodo_pago': [metodo_pago],
            'viaje_id': [viaje_id],
            'latitud_final': [latitud_final],
            'longitud_final': [longitud_final],
            'timestamp': [timestamp],
            'marca_coche': [marca_coche],
            'color_coche': [color_coche],
            'anio_fabricacion': [anio_fabricacion],
            'tipo_combustible': [tipo_combustible],
            'tiene_calefaccion': [tiene_calefaccion],
            'puntos_carnet': [puntos_carnet],
            'cilindraje_motor': [cilindraje_motor],
            'kilometraje': [kilometraje],
            'anio_registro_app': [anio_registro_app],
            'edad_cliente': [edad_cliente],
            'genero_cliente': [genero_cliente],
            'trabajo_cliente': [trabajo_cliente],
            'fecha_registro': [fecha_registro]
        }
        cliente_df = pd.DataFrame(cliente_data)

        prediction = best_model.predict(cliente_df)

        st.subheader('Resultado de la predicción:')
        formatted_prediction = f'Predicción de pago de viaje para el cliente: {prediction[0]:,.2f} €'
        st.write(formatted_prediction)