import pandas as pd
from faker import Faker
import random
import logging

logger = logging.getLogger(__name__)

class FakerDataGenerator:
    def __init__(self, locale='en_US'):
        self.fake = Faker(locale)
        
    def generate_employee_data(self, num_records):
        """Generate synthetic employee data using Faker"""
        logger.info(f"Generating {num_records} synthetic employee records")
        
        departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'Operations']
        positions = ['Manager', 'Senior', 'Junior', 'Lead', 'Associate', 'Director']
        
        employees = []
        for i in range(num_records):
            employee = {
                'employee_id': f"EMP{str(i+1).zfill(6)}",
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'ssn': self.fake.ssn(),
                'department': random.choice(departments),
                'position': random.choice(positions),
                'salary': random.randint(40000, 150000),
                'hire_date': self.fake.date_between(start_date='-10y', end_date='today'),
                'street_address': self.fake.street_address(),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'zip_code': self.fake.zipcode(),
                'manager_id': f"EMP{str(random.randint(1, max(1, i//10))).zfill(6)}" if i > 0 else None,
                'performance_rating': random.choice(['Excellent', 'Good', 'Satisfactory', 'Needs Improvement']),
                'data_source': 'faker'
            }
            employees.append(employee)
        
        logger.info(f"Successfully generated {len(employees)} synthetic records")
        return pd.DataFrame(employees)