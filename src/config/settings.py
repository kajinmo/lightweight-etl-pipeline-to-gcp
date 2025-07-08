import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Google Cloud Settings
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    GCS_BUCKET = os.getenv("GCS_BUCKET")
    BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
    BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
    BIGQUERY_TABLE_ID = os.getenv("BIGQUERY_TABLE_ID")
    
    # Data Processing Settings
    CHUNK_SIZE = 10000
    MAX_RETRIES = 3
    
    @classmethod
    def validate_settings(cls):
        required_settings = [
            'GOOGLE_APPLICATION_CREDENTIALS',
            'GCS_BUCKET_RAW',
            'GCS_BUCKET_PROCESSED',
            'BIGQUERY_PROJECT_ID',
            'BIGQUERY_DATASET_ID',
            'BIGQUERY_TABLE_ID'
        ]
        
        missing = [setting for setting in required_settings if not getattr(cls, setting)]
        if missing:
            raise ValueError(f"Missing required settings: {', '.join(missing)}")

settings = Settings()