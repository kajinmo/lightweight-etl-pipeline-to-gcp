import requests
import pandas as pd
import random
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class APIDataExtractor:
    def __init__(self):
        self.base_url = "https://jsonplaceholder.typicode.com"
        
    def extract_user_data(self, num_records):
        """Extract user data from JSONPlaceholder API and convert to employee format"""
        logger.info(f"Extracting {num_records} records from API")
        
        try:
            # Get users from API
            response = requests.get(f"{self.base_url}/users")
            response.raise_for_status()
            users = response.json()
            
            # If we need more records than available, cycle through the users
            employees = []
            departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'Operations']
            positions = ['Manager', 'Senior', 'Junior', 'Lead', 'Associate', 'Director']
            
            for i in range(num_records):
                user = users[i % len(users)]
                
                # Convert API user to employee format
                employee = {
                    'employee_id': f"API{str(i+1).zfill(6)}",
                    'first_name': user['name'].split()[0],
                    'last_name': user['name'].split()[-1] if len(user['name'].split()) > 1 else 'Unknown',
                    'email': user['email'],
                    'phone': user['phone'],
                    'ssn': f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
                    'department': random.choice(departments),
                    'position': random.choice(positions),
                    'salary': random.randint(40000, 150000),
                    'hire_date': (datetime.now() - timedelta(days=random.randint(1, 3650))).date(),
                    'street_address': user['address']['street'],
                    'city': user['address']['city'],
                    'state': user['address']['zipcode'][:2],  # Mock state from zipcode
                    'zip_code': user['address']['zipcode'],
                    'manager_id': f"API{str(random.randint(1, max(1, i//10))).zfill(6)}" if i > 0 else None,
                    'performance_rating': random.choice(['Excellent', 'Good', 'Satisfactory', 'Needs Improvement']),
                    'data_source': 'api'
                }
                employees.append(employee)
            
            logger.info(f"Successfully extracted {len(employees)} records from API")
            return pd.DataFrame(employees)
            
        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error processing API data: {e}")
            return pd.DataFrame()