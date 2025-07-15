import logging
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd

# Import settings
from src.config.settings import settings

# Import your existing data extractors
from src.data_sources.faker_generator import FakerDataGenerator
from src.data_sources.api_extractor import APIDataExtractor
from src.data_sources.csv_extractor import CSVDataExtractor

# Import new pipeline components
from src.storage.gcs_uploader import GCSUploader
from src.data_processing.data_masking import DataMasker
from src.data_processing.data_validator import DataValidator
from src.bigquery.bigquery_loader import BigQueryLoader


logger = logging.getLogger(__name__)

class PipelineRunner:
    def __init__(self):
        # Initialize data extractors
        self.faker_generator = FakerDataGenerator()
        self.api_extractor = APIDataExtractor()
        self.csv_extractor = CSVDataExtractor()
        
        # Initialize pipeline components
        self.gcs_uploader = GCSUploader()
        self.data_validator = DataValidator()
        self.data_masker = DataMasker()
        self.bigquery_loader = BigQueryLoader()
        
        # Pipeline statistics
        self.pipeline_stats = {
            'start_time': None,
            'end_time': None,
            'sources_processed': 0,
            'total_raw_records': 0,
            'total_valid_records': 0,
            'total_errors': 0,
            'files_uploaded': []
        }
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """Run the complete ETL pipeline"""
        try:
            logger.info("Starting ETL pipeline execution")
            self.pipeline_stats['start_time'] = datetime.now()
            
            # Validate settings
            settings.validate_settings()
            
            # Step 1: Extract data from all sources
            dataframes = self._extract_data()
            
            # Step 2: Upload raw data to GCS
            raw_files = self._upload_raw_data(dataframes)
            
            # Step 3: Process data (validate + mask)
            processed_files = self._process_data(raw_files)
            
            # Step 4: Load to BigQuery
            print("starting step4:load")
            self._load_to_bigquery(processed_files)
            print("finishing step4:load")

            # Finalize pipeline
            self.pipeline_stats['end_time'] = datetime.now()
            self.pipeline_stats['duration'] = (
                self.pipeline_stats['end_time'] - self.pipeline_stats['start_time']
            ).total_seconds()
            
            logger.info("ETL pipeline completed successfully")
            return self._get_pipeline_summary()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            self.pipeline_stats['end_time'] = datetime.now()
            self.pipeline_stats['error'] = str(e)
            raise
    
    def _extract_data(self) -> Dict[str, pd.DataFrame]:
        """Extract data from all sources"""
        logger.info("Extracting data from sources")
        
        dataframes = {}
        
        try:
            # Extract from Faker
            logger.info("Extracting data from Faker")
            df_faker = self.faker_generator.generate_employee_data(num_records=50)
            dataframes['faker'] = df_faker
            logger.info(f"Faker data extracted: {len(df_faker)} records")
            
            # Extract from API
            logger.info("Extracting data from API")
            df_api = self.api_extractor.extract_user_data(num_records=50)
            dataframes['api'] = df_api
            logger.info(f"API data extracted: {len(df_api)} records")
            
            # Extract from CSV
            logger.info("Extracting data from CSV")
            df_csv = self.csv_extractor.extract_csv_data(num_records=50)
            dataframes['csv'] = df_csv
            logger.info(f"CSV data extracted: {len(df_csv)} records")
            
            # Update statistics
            self.pipeline_stats['sources_processed'] = len(dataframes)
            self.pipeline_stats['total_raw_records'] = sum(len(df) for df in dataframes.values())
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise
    
    def _upload_raw_data(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Upload raw DataFrames to GCS"""
        logger.info("Uploading raw data to GCS")
        
        raw_files = {}
        
        try:
            for source_name, df in dataframes.items():
                logger.info(f"Uploading raw data for {source_name}")
                filename = self.gcs_uploader.upload_data(df, data_type="raw", source_name=source_name)
                raw_files[source_name] = filename
                self.pipeline_stats['files_uploaded'].append(f"raw/{filename}")
            
            logger.info(f"Raw data upload completed: {len(raw_files)} files")
            return raw_files
            
        except Exception as e:
            logger.error(f"Error uploading raw data: {str(e)}")
            raise
    
    def _process_data(self, raw_files: Dict[str, str]) -> Dict[str, str]:
        """Process data: validate and mask"""
        logger.info("Processing data (validation + masking)")
        
        processed_files = {}
        
        try:
            for source_name, filename in raw_files.items():
                logger.info(f"Processing data for {source_name}")
                
                # Download raw data
                df_raw = self.gcs_uploader.download_data(filename)
                
                # Validate data
                is_valid, validation_issues = self.data_validator.validate_dataframe(df_raw)
            
                # Upload processed data
                if is_valid:
                    df_validated = df_raw
                else:
                    logger.error("Validation failed. Details:")
                    for issue in validation_issues:
                        logger.error(f"- {issue}")
                    df_validated = pd.DataFrame() # empty to avoid downstream errors
                
                if not df_validated.empty:
                    # Validation only allows if all lines are correct
                    # TODO: Apply data masking here
                    df_masked = self.data_masker.mask_sensitive_data(df_validated)
                    df_processed = df_masked
                    processed_filename = self.gcs_uploader.upload_data(
                        df=df_processed,
                        data_type="processed",
                        source_name=source_name
                    )
                    processed_files[source_name] = processed_filename
                    self.pipeline_stats['files_uploaded'].append(f"processed/{processed_filename}")
                    
                    # Update statistics
                    self.pipeline_stats['total_valid_records'] += len(df_processed)                
            
            # Update error statistics
            validation_summary = self.data_validator.get_validation_summary()
            self.pipeline_stats['total_errors'] = validation_summary['total_errors']
            self.pipeline_stats['validation_summary'] = validation_summary
            
            logger.info(f"Data processing completed: {len(processed_files)} files")
            return processed_files
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise
    
    def _load_to_bigquery(self, processed_files: Dict[str, str]):
        """Load processed data to BigQuery"""
        logger.info("Loading data to BigQuery")
        
        try:
            for source_name, filename in processed_files.items():
                logger.info(f"Loading {source_name} data to BigQuery")
                job_id = self.bigquery_loader.load_from_gcs_parquet(filename, source_name)
                logger.info(f"BigQuery load job started: {job_id}")
            
            logger.info("BigQuery loading completed")
            
        except Exception as e:
            logger.error(f"Error loading to BigQuery: {str(e)}")
            raise
    
    def _get_pipeline_summary(self) -> Dict[str, Any]:
        """Get pipeline execution summary"""
        summary = {
            'pipeline_stats': self.pipeline_stats,
            'validation_summary': self.data_validator.get_validation_summary(),
            'bigquery_info': None
        }
        
        try:
            summary['bigquery_info'] = self.bigquery_loader.get_table_info()
        except Exception as e:
            logger.warning(f"Could not get BigQuery table info: {str(e)}")
        
        return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    runner = PipelineRunner()
    summary = runner.run_full_pipeline()
    print("Pipeline execution summary:")
    print(summary)