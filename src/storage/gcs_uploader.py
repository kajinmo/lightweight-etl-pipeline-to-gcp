import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.api_core.exceptions import Conflict
from datetime import datetime
import logging
from typing import Optional
import io
import os
from src.config.settings import settings

logger = logging.getLogger(__name__)

class GCSUploader:
    def __init__(self, location: str = "us-east1"):
        self.client = storage.Client()
        self.bucket_name = settings.GCS_BUCKET
        #from dotenv import load_dotenv
        #load_dotenv()
        #self.bucket_name = os.getenv("GCS_BUCKET")
        self.bucket = self._get_or_create_bucket(location)
        

    def _generate_filename(self, source_name: str, data_type: str = "raw") -> str:
        """Generate timestamped filename and blob path for data versioning"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{data_type}/{source_name}_{timestamp}.parquet"

    def _get_or_create_bucket(self, location="us-east1"):
        """Create bucket if not exists"""
        existing_buckets = [b.name for b in self.client.list_buckets()]
        if self.bucket_name in existing_buckets:
            logger.info(f"Bucket '{self.bucket_name}' already exists.") #DESCOMENTAR
            #print(f"Bucket '{self.bucket_name}' already exists.")
            return self.client.bucket(self.bucket_name)
        try:
            bucket = self.client.bucket(self.bucket_name)
            self.client.create_bucket(bucket, location=location)
            logger.info(f"Bucket '{self.bucket_name}' created in region {location}.")
            return bucket
        except Conflict:
            logger.warning(f"Bucket '{self.bucket_name}' already exists or conflict (maybe already exists).")
            return self.client.bucket(self.bucket_name)

    def upload_data(self, df: pd.DataFrame, data_type: str = "raw", source_name: str = None) -> str:
        """Uploads a DataFrame as Parquet for raw/ or processed/"""
        try:
            # force zip_code as string for CSV data
            if source_name == "csv" and "zip_code" in df.columns:
                df["zip_code"] = df["zip_code"].astype(str)
            
            # Convert hire_date to date
            if "hire_date" in df.columns:
                df["hire_date"] = pd.to_datetime(df["hire_date"])
                df["hire_date"] = df["hire_date"].dt.date

            # to prevent problems for REQUIRED fields in BigQuery
            required_fields = ['employee_id', 'first_name', 'last_name', 'email']
            for col in required_fields:
                if col in df.columns:
                    df[col] = df[col].fillna("").astype(str).str.strip()
                    df = df[df[col] != ""]
            
            # Check if there are still nulls left
            assert df[required_fields].isnull().sum().sum() == 0, "Ainda há nulos em campos obrigatórios"

            # Parquet with explicit schema to avoid erroneous inference
            table = pa.Table.from_pandas(df, preserve_index=False)

            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer)
            parquet_buffer.seek(0)

            filename = self._generate_filename(source_name, data_type)
            blob = self.bucket.blob(filename)
            blob.upload_from_string(parquet_buffer.read(), content_type='application/octet-stream')
            logger.info(f"{data_type.title()} data uploaded: gs://{self.bucket.name}/{filename}")
            return filename
        except Exception as e:
            logger.error(f"Erro ao enviar {data_type} data para {source_name}: {e}")
            raise
    
    def download_data(self, filename: str) -> pd.DataFrame:
        """Download raw parquet file from GCS"""
        try:
            blob = self.bucket.blob(filename)
            parquet_data = blob.download_as_bytes()
            if not parquet_data:
                raise ValueError(f"O arquivo {filename} está vazio no bucket.")
            df = pd.read_parquet(io.BytesIO(parquet_data))
            logger.info(f"Arquivo baixado com sucesso: {filename}")
            return df
        except Exception as e:
            logger.error(f"Erro ao baixar arquivo {filename}: {e}")
            raise

    def list_files(self, data_type: str = "raw", source_name: Optional[str] = None) -> list:
        """List files in GSC bucket inside raw/ ou processed/"""
        prefix = f"{data_type}/"
        if source_name:
            prefix += source_name
        try:
            blobs = self.bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs if blob.name.endswith('.parquet')]
        except Exception as e:
            logger.error(f"Erro ao listar arquivos ({data_type}): {e}")
            raise


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    uploader = GCSUploader()

    df_test = pd.DataFrame({
        "id": [1, 2],
        "nome": ["Ana", "Joao"]
    })

    filename = uploader.upload_data(df_test, data_type="raw", source_name="api")
    print(f"Enviado para: {filename}")

    arquivos = uploader.list_files(data_type="raw", source_name="api")
    print("Arquivos encontrados:", arquivos)

    if arquivos:
        df_baixado = uploader.download_data(arquivos[-1])
        print(df_baixado.head())