from unicodedata import name
from io import BytesIO
import pandas as pd

from prefect import task
from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client

@task(name="get file from silver")
def get_file_from_silver(object_name: str):
    """
    Get file from silver
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    response = client.get_object(BUCKET_SILVER, object_name)

    return response.read()

@task(name="get file from gold")
def get_file_from_gold(object_name: str):
    """
    Get file from gold
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    response = client.get_object(BUCKET_GOLD, object_name)

    return response.read()


@task(name="create new kpi file")
def create_new_kpi_file(file_name: str):
    """
    Used to store all the kpi data
    """
    df = pd.DataFrame()
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    put_object(buffer.getvalue(), f'{file_name}.parquet')

@task(name="get purchases file")
def get_purchases_file():
    file_name = "achats.parquet"
    file = get_file_from_silver(file_name)
    return {
        "file": file,
        "file_name": file_name
    }

@task(name="get clients file")
def get_clients_file():
    file_name = "clients.parquet"
    file = get_file_from_silver(file_name)
    return {
        "file": file,
        "file_name": file_name
    }

@task(name="put object")
def put_object(file: bytes, file_name: str):
    """
    Used to store file to MinIO
    """
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

    client.put_object(
        BUCKET_GOLD,
        file_name,
        BytesIO(file),
        length=len(file)
    )

@task(name="put files")
def put_clients_purchases_files():
    """
    Store the two files from silver
    """
    purchases = get_purchases_file()
    clients = get_clients_file()

    put_object(purchases["file"], purchases["file_name"])
    put_object(clients["file"], clients["file_name"])

@task(name="amount per month")
def amount_per_month():
    """
    Create a file where the amount per month is agregate
    """
    file = get_purchases_file()["file"]
    df = pd.read_parquet(BytesIO(file))

    #convert object to datetime
    df['date_achat'] = pd.to_datetime(df['date_achat'])

    #create a new column with the same months and years
    df['year_month'] = df['date_achat'].dt.to_period('M').astype(str)
    
    #group the amounts per month
    result = df.groupby('year_month')['montant'].sum().reset_index()
    
    #create a file with the result and put it to gold
    buffer = BytesIO()
    result.to_parquet(buffer, index=False)
    put_object(buffer.getvalue(), 'amount_per_month.parquet')
    
    return result

def growth_rate_per_month():
    """
    Create a file where the amount per month is agregate
    """
    file_name = "amount_per_month.parquet"
    file = get_file_from_gold(file_name)
    df = pd.read_parquet(BytesIO(file))
    df['taux_croissance'] = df['montant'].pct_change() * 100
    print(df['taux_croissance'])

    #create a file with the result and put it to gold
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    put_object(buffer.getvalue(), file_name)

@task(name="kpi transformation")
def kpi_transformation():
    amount_per_month()
    growth_rate_per_month()

if __name__ == "__main__":
    put_clients_purchases_files()
    kpi_transformation()