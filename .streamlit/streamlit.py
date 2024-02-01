import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/balmagostr/Documents/GitHub/CochePole/.streamlit/credentials.json"
