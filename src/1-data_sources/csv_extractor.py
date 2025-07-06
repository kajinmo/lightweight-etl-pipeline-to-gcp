import pandas as pd
import os
import logging
from faker import Faker

logger = logging.getLogger(__name__)

class CSVDataExtractor:
    def __init__(self, csv_path='data/sample_employees.csv'):
        self.csv_path = csv_path
        self.fake = Faker()
        
    def create_sample_csv(self, num_records=100):
        """Create a sample CSV file if it doesn't exist"""
        if not os.path.exists(self.csv_path):
            logger.info(f"Creating sample CSV file at {self.csv_path}")
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
            
            # Generate sample data
            departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'Operations']
            positions = ['Manager', 'Senior', 'Junior', 'Lead', 'Associate', 'Director']
            
            employees = []
            for i in range(num_records):
                employee = {
                    'employee_id': f"CSV{str(i+1).zfill(6)}",
                    'first_name': self.fake.first_name(),
                    'last_name': self.fake.last_name(),
                    'email': self.fake.email(),
                    'phone': self.fake.phone_number(),
                    'ssn': self.fake.ssn(),
                    'department': self.fake.random_element(departments),
                    'position': self.fake.random_element(positions),
                    'salary': self.fake.random_int(40000, 150000),
                    'hire_date': self.fake.date_between(start_date='-10y', end_date='today'),
                    'street_address': self.fake.street_address(),
                    'city': self.fake.city(),
                    'state': self.fake.state(),
                    'zip_code': self.fake.zipcode(),
                    'manager_id': f"CSV{str(self.fake.random_int(1, max(1, i//10))).zfill(6)}" if i > 0 else None,
                    'performance_rating': self.fake.random_element(['Excellent', 'Good', 'Satisfactory', 'Needs Improvement']),
                    'data_source': 'csv'
                }
                employees.append(employee)
            
            df = pd.DataFrame(employees)
            df.to_csv(self.csv_path, index=False)
            logger.info(f"Sample CSV created with {len(employees)} records")
    
    def extract_csv_data(self, num_records):
        """Extract data from CSV file"""
        logger.info(f"Extracting {num_records} records from CSV")
        
        try:
            # Create sample CSV if it doesn't exist
            self.create_sample_csv()
            
            # Read CSV
            df = pd.read_csv(self.csv_path)
            
            # Sample required number of records
            if len(df) >= num_records:
                df = df.sample(n=num_records, random_state=42)
            else:
                logger.warning(f"CSV contains only {len(df)} records, requested {num_records}")
            
            logger.info(f"Successfully extracted {len(df)} records from CSV")
            return df
            
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return pd.DataFrame()