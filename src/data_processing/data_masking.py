import pandas as pd
import hashlib
import logging
import re

logger = logging.getLogger(__name__)

class DataMasker:
    def __init__(self, salt="etl_pipeline_salt"):
        self.salt = salt
        
    def _tokenize_field(self, value, field_name):
        """Simple tokenization using hash with salt"""
        if pd.isna(value) or value == '':
            return value
        
        # Create a hash of the value with salt and field name
        hash_input = f"{self.salt}_{field_name}_{str(value)}".encode('utf-8')
        hash_value = hashlib.sha256(hash_input).hexdigest()
        
        # Return first 8 characters as token with prefix
        return f"TOKEN_{hash_value[:8].upper()}"
    
    def _mask_email(self, email):
        """Mask email by tokenizing the local part"""
        if pd.isna(email) or email == '' or '@' not in email:
            return email
        
        local_part, domain = email.split('@', 1)
        tokenized_local = self._tokenize_field(local_part, 'email')
        return f"{tokenized_local}@{domain}"
    
    def _mask_phone(self, phone):
        """Mask phone number by keeping format but tokenizing digits"""
        if pd.isna(phone) or phone == '':
            return phone
        
        # Extract digits only
        digits = re.sub(r'\D', '', str(phone))
        if len(digits) >= 10:
            # Keep last 4 digits, tokenize the rest
            tokenized = self._tokenize_field(digits[:-4], 'phone')
            return f"***-***-{digits[-4:]}"
        else:
            return self._tokenize_field(phone, 'phone')
    
    def mask_sensitive_data(self, df):
        """Mask sensitive information in the dataframe"""
        logger.info("Starting data masking process")
        
        if df.empty:
            logger.warning("Empty dataframe provided for masking")
            return df
        
        df_masked = df.copy()
        
        # Define sensitive fields and their masking methods
        sensitive_fields = {
            'ssn': lambda x: self._tokenize_field(x, 'ssn'),
            'salary': lambda x: self._tokenize_field(x, 'salary'),
            'email': self._mask_email,
            'phone': self._mask_phone,
            'street_address': lambda x: self._tokenize_field(x, 'address')
        }
        
        # Apply masking to each sensitive field
        for field, mask_func in sensitive_fields.items():
            if field in df_masked.columns:
                logger.info(f"Masking field: {field}")
                df_masked[field] = df_masked[field].apply(mask_func)
        
        # Add masking metadata
        df_masked['masked_at'] = pd.Timestamp.now()
        df_masked['is_masked'] = True
        
        logger.info(f"Data masking completed for {len(df_masked)} records")
        return df_masked