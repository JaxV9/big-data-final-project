from fastapi import FastAPI
from flows.config import BUCKET_GOLD, get_minio_client
import pandas as pd
from io import BytesIO
import os
from pymongo import MongoClient

MONGO_URL = os.getenv("MONGO_URL", "mongodb://user:pass@mongodb:27017/")
client = MongoClient(MONGO_URL)
db = client["mongo"]

app = FastAPI()

def get_file_from_gold(object_name: str):
    """
    Get file from gold
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    response = client.get_object(BUCKET_GOLD, object_name)

    return response.read()

async def insertDataToMongo(file: bytes, collection_name: str):
    client = db[collection_name]
    df = pd.read_parquet(BytesIO(file))

    records = df.to_dict('records')

    # transform Null values to None
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    result = client.insert_many(records)

    print(f"inserted_count: {result.inserted_ids}")
    return result

async def readDataToMongo(collection_name: str):
    client = db[collection_name]
    kpis = list(client.find({}, {"_id": 0}))

    return kpis

@app.get("/")
async def root():
    file = get_file_from_gold('amount_per_month.parquet')
    df = pd.read_parquet(BytesIO(file))
    
    value = df['taux_croissance'][1]
    if pd.isna(value):
        value = None
    
    kpi = await readDataToMongo("amount_per_month")
    
    return {
        "colonnes": df.columns.tolist(),
        "nombre_lignes": len(df),
        "nombre_colonnes": len(df.columns),
        "types": df.dtypes.astype(str).to_dict(),
        "year_mont": df['year_month'][0:13],
        "kpi": kpi
    }

@app.get("/amount_per_month")
async def root():    
    kpi = await readDataToMongo("amount_per_month")
    
    return {
        "kpi": kpi
    }