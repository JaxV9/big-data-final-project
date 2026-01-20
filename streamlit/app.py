from requests import request
import streamlit as st
import requests, time
import pandas as pd
from numpy.random import default_rng as rng
from config import BUCKET_GOLD, get_minio_client

def get_api_performance():
    start_time = time.time()
    requests.get("http://localhost:8000/amount_per_month")
    end_time = time.time()
    duration_in_ms = (end_time - start_time) * 1000
    return duration_in_ms

def get_file_from_gold(object_name: str):
    """
    Get file from gold
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    response = client.get_object(BUCKET_GOLD, object_name)

    return response.read()

def generate_api_performance_history():
    history = []
    for i in range(10):
        performance = get_api_performance()
        history.append(performance)

    return history


def minio_performance_history():
    start_time = time.time()
    get_file_from_gold('amount_per_month.parquet')
    end_time = time.time()
    duration_in_ms = (end_time - start_time) * 1000
    
    return duration_in_ms

def generate_minio_performance_history():
    history = []
    for i in range(10):
        performance = minio_performance_history()
        history.append(performance)

    return history

st.set_page_config(page_title="Benchmark: FastAPI request vs MinIO db request", layout="wide")
st.title("Benchmark dashboard")

df = pd.DataFrame({
    "MinIO": minio_performance_history(),
    "FastAPI": generate_api_performance_history()
})
st.bar_chart(df)
