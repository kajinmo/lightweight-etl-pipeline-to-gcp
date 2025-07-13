# tests/unit/test_data_validator.py
import pytest
import pandas as pd
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
from data_processing.data_validator import DataValidator, EmployeeModel


@pytest.fixture
def valid_employee_data():
    return {
        "employee_id": "EMP001",
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@example.com",
        "department": "Engineering",
        "position": "Developer",
        "hire_date": datetime(2020, 1, 1),
        "data_source": "test"
    }

@pytest.fixture
def sample_dataframe(valid_employee_data):
    return pd.DataFrame([valid_employee_data])

def test_employee_model_validation(valid_employee_data):
    """Testa a validação do modelo Pydantic"""
    employee = EmployeeModel(**valid_employee_data)
    assert employee.employee_id == "EMP001"

def test_valid_dataframe(sample_dataframe):
    """Testa validação de DataFrame válido"""
    validator = DataValidator()
    is_valid, errors = validator.validate_dataframe(sample_dataframe)
    assert is_valid
    assert not errors

def test_missing_required_field(sample_dataframe):
    """Testa detecção de campo obrigatório faltante"""
    validator = DataValidator()
    df_missing = sample_dataframe.drop(columns=["email"])
    is_valid, errors = validator.validate_dataframe(df_missing)
    assert not is_valid
    assert "Missing required columns" in errors[0]

def test_invalid_email(sample_dataframe):
    """Testa validação de e-mail inválido"""
    validator = DataValidator()
    df_invalid = sample_dataframe.copy()
    df_invalid.at[0, "email"] = "invalid-email"
    is_valid, errors = validator.validate_dataframe(df_invalid)
    assert not is_valid
    assert "Formato de e-mail inválido" in errors[0]

def test_null_values_in_required_fields(sample_dataframe):
    """Testa detecção de valores nulos em campos obrigatórios"""
    validator = DataValidator()
    df_with_nulls = sample_dataframe.copy()
    df_with_nulls.at[0, "first_name"] = None
    is_valid, errors = validator.validate_dataframe(df_with_nulls)
    assert not is_valid
    assert "valores nulos" in errors[0].lower()