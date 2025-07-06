import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # GCP Configuration
    #...

    # Data Configuration
    #...

    # Email Configuration
    #...

    # Scheduling
    #SCHEDULE_TIME = os.getenv('SCHEDULE_TIME', '10:00')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'logs/etl_pipeline.log')
    
    # Retry Configuration
    #...