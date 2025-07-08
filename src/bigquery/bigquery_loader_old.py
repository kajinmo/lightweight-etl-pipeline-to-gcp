import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import time

logger = logging.getLogger(__name__)

class BigQueryLoader:
    def __init__(self, project_id, dataset_id, table_id, service_account_path):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.service_account_path = service_account_path
        self.client = None
        self.table_ref = None
        
    def _initialize_client(self):
        """Initialize BigQuery client"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.service_account_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)
            self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
            logger.info("BigQuery client initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            return False
    
    def create_dataset_if_not_exists(self):
        """Create dataset if it doesn't exist"""
        try:
            if not self.client:
                if not self._initialize_client():
                    return False
            
            dataset_ref = self.client.dataset(self.dataset_id)
            
            try:
                self.client.get_dataset(dataset_ref)
                logger.info(f"Dataset {self.dataset_id} already exists")
            except Exception:
                # Dataset doesn't exist, create it
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Free tier is available in US
                dataset.description = "Employee data ETL pipeline dataset"
                
                dataset = self.client.create_dataset(dataset)
                logger.info(f"Created dataset {self.dataset_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
            return False
    
    def create_table_if_not_exists(self, df):
        """Create table if it doesn't exist"""
        try:
            if not self.client:
                if not self._initialize_client():
                    return False
            
            try:
                self.client.get_table(self.table_ref)
                logger.info(f"Table {self.table_id} already exists")
            except Exception:
                # Table doesn't exist, create it
                # Define schema based on dataframe
                schema = []
                for column in df.columns:
                    dtype = str(df[column].dtype)
                    if 'int' in dtype:
                        bq_type = 'INTEGER'
                    elif 'float' in dtype:
                        bq_type = 'FLOAT'
                    elif 'bool' in dtype:
                        bq_type = 'BOOLEAN'
                    elif 'datetime' in dtype:
                        bq_type = 'TIMESTAMP'
                    else:
                        bq_type = 'STRING'
                    
                    schema.append(bigquery.SchemaField(column, bq_type))
                
                table = bigquery.Table(self.table_ref, schema=schema)
                table.description = "Employee data with masked sensitive information"
                
                table = self.client.create_table(table)
                logger.info(f"Created table {self.table_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            return False
    
    def load_data(self, df, write_disposition='WRITE_APPEND'):
        """Load data into BigQuery table"""
        try:
            if df.empty:
                logger.warning("Empty dataframe provided for loading")
                return False
            
            if not self.client:
                if not self._initialize_client():
                    return False
            
            # Create dataset and table if they don't exist
            if not self.create_dataset_if_not_exists():
                return False
            
            if not self.create_table_if_not_exists(df):
                return False
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=True,
                create_disposition='CREATE_IF_NEEDED'
            )
            
            # Load data
            logger.info(f"Loading {len(df)} records into BigQuery")
            load_job = self.client.load_table_from_dataframe(
                df, self.table_ref, job_config=job_config
            )
            
            # Wait for job to complete
            load_job.result()
            
            if load_job.state == 'DONE':
                if load_job.errors:
                    logger.error(f"Load job completed with errors: {load_job.errors}")
                    return False
                else:
                    logger.info(f"Successfully loaded {len(df)} records to BigQuery")
                    return True
            else:
                logger.error(f"Load job failed with state: {load_job.state}")
                return False
                
        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {e}")
            return False
    
    def query_data(self, query):
        """Execute query and return results"""
        try:
            if not self.client:
                if not self._initialize_client():
                    return None
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Convert to pandas DataFrame
            df = results.to_dataframe()
            logger.info(f"Query executed successfully, returned {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return None