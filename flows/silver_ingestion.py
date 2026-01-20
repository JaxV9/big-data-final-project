from io import BytesIO
import pandas as pd

from prefect import task
from config import BUCKET_SILVER, BUCKET_BRONZE, get_minio_client

@task(name="get file from bronze")
def get_file_from_bronze(object_name: str):
    """
    Get file from bronze
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)
    
    response = client.get_object(BUCKET_BRONZE, object_name)

    return response.read()

@task(name="convert csv file to parquet")
def convert_csv_to_parquet(data: bytes):

    csv_data = BytesIO(data)
    df = pd.read_csv(csv_data)

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy")
    parquet_buffer.seek(0)
    return parquet_buffer.read()

@task(name="normalize date format")
def normalize_date_format(parquet_file: bytes):
    """
    Convert the purchases file prices to int
    """

    file = BytesIO(parquet_file)
    df = pd.read_parquet(file)
    
    # Convertir : supprimer l'heure, garder juste la date
    df["date_achat"] = pd.to_datetime(df['date_achat']).dt.strftime('%Y-%m-%d')

    output = BytesIO()
    df.to_parquet(output, engine="pyarrow", compression="snappy")
    output.seek(0)
    return output.read()

@task(name="put object")
def put_object(file: bytes, file_name: str):
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    client.put_object(
        BUCKET_SILVER,
        file_name,
        BytesIO(file),
        length=len(file)
    )

@task(name="transform files")
def transform_files():
    
    purchases_name = "achats.parquet"
    clients_name = "clients.parquet"

    purchases_data = get_file_from_bronze("achats.csv")
    clients_data = get_file_from_bronze("clients.csv")

    purchases = convert_csv_to_parquet(purchases_data)
    clients = convert_csv_to_parquet(clients_data)

    purchases = normalize_date_format(purchases)

    put_object(purchases, purchases_name)
    put_object(clients, clients_name)

    return {
        "clients": purchases_name,
        "achats": clients_name
    }


if __name__ == "__main__":
    result = transform_files()
    print(f"Silver ingestion complete: {result}")