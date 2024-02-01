import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/balmagostr/Documents/GitHub/CochePole/.streamlit/credentials.json"

# Inicializa el cliente de BigQuery
client = bigquery.Client()

# Consulta los datos desde BigQuery
query = """
SELECT *
FROM 
"""

# Ejecuta la consulta
query_job = client.query(query)

# Muestra los resultados en Streamlit
st.write("Datos de BigQuery")
for row in query_job:
    st.write(row)


