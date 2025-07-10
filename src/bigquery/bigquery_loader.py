from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import pandas as pd
import logging
from typing import Optional, List
from src.config.settings import settings

logger = logging.getLogger(__name__)

class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client(project=settings.BIGQUERY_PROJECT_ID)
        self.dataset_id = settings.BIGQUERY_DATASET_ID
        self.table_id = settings.BIGQUERY_TABLE_ID
        self.full_table_id = f"{settings.BIGQUERY_PROJECT_ID}.{self.dataset_id}.{self.table_id}"
    
    def create_dataset_if_not_exists(self):
        """Create BigQuery dataset if it doesn't exist"""
        try:
            dataset_ref = self.client.dataset(self.dataset_id)
            try:
                self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {self.dataset_id} already exists")
            except GoogleCloudError:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # or your preferred location
                dataset = self.client.create_dataset(dataset)
                logger.info(f"Created dataset {self.dataset_id}")
        except Exception as e:
            logger.error(f"Error creating dataset: {str(e)}")
            raise
    
    def create_table_if_not_exists(self):
        """Create BigQuery table if it doesn't exist"""
        try:
            table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
            try:
                self.client.get_table(table_ref)
                logger.info(f"Table {self.table_id} already exists")
            except GoogleCloudError:
                # Define schema
                schema = [
                    bigquery.SchemaField("employee_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("ssn", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("department", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("position", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("salary", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("hire_date", "DATE", mode="NULLABLE"),
                    bigquery.SchemaField("street_address", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("zip_code", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("manager_id", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("performance_rating", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("data_source", "STRING", mode="NULLABLE"),
                ]
                
                table = bigquery.Table(table_ref, schema=schema)
                table = self.client.create_table(table)
                logger.info(f"Created table {self.table_id}")
        except Exception as e:
            logger.error(f"Error creating table: {str(e)}")
            raise
    
    def load_from_gcs_parquet(self, gcs_uri: str, source_name: str) -> str:
        """Load data from GCS parquet file to BigQuery"""
        try:
            # Ensure dataset and table exist
            self.create_dataset_if_not_exists()
            self.create_table_if_not_exists()
            
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False,  # Use predefined schema
                # Add metadata
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ]
            )
            
            # Construct full GCS URI
            full_gcs_uri = f"gs://{settings.GCS_BUCKET}/{gcs_uri}"
            
            # Start the load job
            load_job = self.client.load_table_from_uri(
                full_gcs_uri,
                self.full_table_id,
                job_config=job_config
            )
            
            # Wait for job to complete
            load_job.result()
            
            # Get job statistics
            destination_table = self.client.get_table(self.full_table_id)
            rows_loaded = load_job.output_rows
            
            logger.info(f"Successfully loaded {rows_loaded} rows from {gcs_uri} to {self.full_table_id}")
            logger.info(f"Total rows in table: {destination_table.num_rows}")
            
            return load_job.job_id
            
        except Exception as e:
            logger.error(f"Error loading data from GCS to BigQuery: {str(e)}")
            raise
    def query_table(self, query: Optional[str] = None) -> pd.DataFrame:
        """Query the BigQuery table"""
        try:
            if query is None:
                query = f"SELECT * FROM `{self.full_table_id}` LIMIT 100"
            
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error querying BigQuery table: {str(e)}")
            raise
    
    def get_table_info(self) -> dict:
        """Get table information"""
        try:
            table = self.client.get_table(self.full_table_id)
            return {
                'table_id': table.table_id,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'schema': [{'name': field.name, 'type': field.field_type} for field in table.schema]
            }
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            raise

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()