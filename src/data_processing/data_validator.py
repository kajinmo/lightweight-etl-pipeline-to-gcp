import pandas as pd
import logging
from datetime import datetime
from pydantic import BaseModel, field_validator, Field, ConfigDict
from typing import Optional, List, Dict, Any
import re

logger = logging.getLogger(__name__)

class EmployeeModel(BaseModel):
    """Modelo Pydantic v2 para validação de dados de funcionários"""
    model_config = ConfigDict(extra='forbid')  # Bloqueia campos extras

    employee_id: str = Field(..., min_length=6, max_length=10)
    first_name: str = Field(..., min_length=2, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    email: str
    department: str
    position: str
    hire_date: datetime
    data_source: str
    phone: Optional[str] = None
    ssn: Optional[str] = None
    salary: Optional[float] = Field(None, ge=0)
    street_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    manager_id: Optional[str] = None
    performance_rating: Optional[str] = None

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Valida formato de e-mail"""
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', v):
            raise ValueError('Formato de e-mail inválido')
        return v.lower().strip()

    @field_validator('hire_date')
    @classmethod
    def validate_hire_date(cls, v: datetime) -> datetime:
        """Valida formato de data"""
        if v > datetime.now():
            raise ValueError('Data de contratação não pode estar no futuro')
        return v

class DataValidator:
    def __init__(self):
        self.required_fields = [
            name for name, field in EmployeeModel.model_fields.items()
            if field.is_required()
        ]
        self.validation_errors = []

    def validate_dataframe(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        logger.info("Starting data validation")

        if df.empty:
            logger.error("Empty DataFrame provided for validation")
            return False, ["Empty DataFrame"]

        validation_issues = []

        # Checks required columns
        missing_cols = [col for col in self.required_fields if col not in df.columns]
        if missing_cols:
            msg = f"Missing required columns: {missing_cols}"
            logger.error(msg)
            validation_issues.append(msg)
            return False, validation_issues

        # Validates each record
        for idx, record in df.iterrows():
            try:
                EmployeeModel.model_validate(record.to_dict())
            except Exception as e:
                msg = f"Linha {idx + 1}: {str(e)}"
                logger.warning(msg)
                validation_issues.append(msg)

        # Checks for duplicate IDs
        if 'employee_id' in df.columns and df['employee_id'].duplicated().any():
            dups = df['employee_id'].duplicated().sum()
            msg = f"Duplicate IDs found: {dups} casos"
            logger.warning(msg)
            validation_issues.append(msg)

        if not validation_issues:
            logger.info("Validation completed without errors")
            return True, []

        logger.warning(f"Validation found {len(validation_issues)} problems")
        return False, validation_issues

    def get_validation_summary(self) -> Dict[str, Any]:
        """Get summary of validation results"""
        return {
            'total_errors': len(self.validation_errors),
            'errors_by_source': self._group_errors_by_source(),
            'common_errors': self._get_common_errors()
        }
    
    def _group_errors_by_source(self) -> Dict[str, int]:
        """Group errors by data source"""
        source_errors = {}
        for error in self.validation_errors:
            source = error['source']
            source_errors[source] = source_errors.get(source, 0) + 1
        return source_errors
    
    def _get_common_errors(self) -> List[Dict[str, Any]]:
        """Get most common validation errors"""
        error_counts = {}
        for error in self.validation_errors:
            for validation_error in error['errors']:
                error_msg = validation_error.get('msg', 'Unknown error')
                error_counts[error_msg] = error_counts.get(error_msg, 0) + 1
        
        # Sort by frequency and return top 10
        sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'error': error, 'count': count} for error, count in sorted_errors[:10]]