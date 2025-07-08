
import logging
from google.cloud import storage
from google.api_core.exceptions import Conflict
logger = logging.getLogger(__name__)
import os
from dotenv import load_dotenv
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

client = storage.Client()

def create_bucket_if_not_exists(self, location="us-east1"):
    """Create bucket if not exists"""
    existing_buckets = [b.name for b in self.client.list_buckets()]

    if self.bucket_name in existing_buckets:
        logger.info(f"Bucket '{self.bucket_name}' already exists.")
        return self.client.bucket(self.bucket_name)
    try:
        bucket = self.client.bucket(self.bucket_name)
        self.client.create_bucket(bucket, location=location)
        logger.info(f"Bucket '{self.bucket_name}' created in region {location}.")
        return bucket
    except Conflict:
        logger.warning(f"Bucket '{self.bucket_name}' already exists or conflict.")
        return self.client.bucket(self.bucket_name)
        

create_bucket_if_not_exists(location="us-east1")
"""
def create_bucket_if_not_exists(bucket_name, location="us-east1"):
    client = storage.Client()

    # Check if bucket already exists
    buckets = [b.name for b in client.list_buckets()]
    if bucket_name in buckets:
        print(f"Bucket '{bucket_name}' already exists.")
        return

    # Create bucket
    try:
        bucket = client.bucket(bucket_name)
        new_bucket = client.create_bucket(bucket, location=location)
        print(f"Bucket '{bucket_name}' created in the region {location}.")
    except Conflict:
        print(f"Bucket '{bucket_name}' already exists (conflict).")

def upload_arquivo(bucket_name, arquivo_local, nome_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(nome_blob)
    blob.upload_from_filename(arquivo_local)
    print(f"Arquivo '{arquivo_local}' enviado para 'gs://{bucket_name}/{nome_blob}'.")

# Exemplo de uso
bucket_name = "meu-bucket-dados-luis"  # precisa ser único globalmente
create_bucket_if_not_exists(bucket_name)
upload_arquivo(bucket_name, "data/test_employees.csv", "pastablob/test_employees.csv")
"""
