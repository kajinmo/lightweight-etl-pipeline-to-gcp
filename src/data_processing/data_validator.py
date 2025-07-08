import pandas as pd
import logging
from datetime import datetime
from pydantic import BaseModel, field_validator, Field, ConfigDict
from typing import Optional, List
import re

logger = logging.getLogger(__name__)

class EmployeeModel(BaseModel):
    """Modelo Pydantic v2 para validação de dados de funcionários"""
    model_config = ConfigDict(extra='forbid')  # Bloqueia campos extras

    employee_id: str = Field(..., min_length=6, max_length=10)
    first_name: str = Field(..., min_length=2, max_length=50)
    last_name: str = Field(..., min_length=2, max_length=50)
    email: str
    department: str
    position: str
    hire_date: str
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
    def validate_hire_date(cls, v: str) -> str:
        """Valida formato de data"""
        try:
            pd.to_datetime(v)
            return v
        except Exception as e:
            raise ValueError(f'Data inválida: {str(e)}')

class DataValidator:
    def __init__(self):
        # Obtém campos obrigatórios do modelo
        self.required_fields = [
            name for name, field in EmployeeModel.model_fields.items() 
            if not field.is_required() or field.default is None
        ]

    def validate_dataframe(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        """Valida um DataFrame usando o modelo Pydantic"""
        logger.info("Iniciando validação de dados")
        
        if df.empty:
            logger.error("DataFrame vazio fornecido para validação")
            return False, ["DataFrame vazio"]
        
        validation_issues = []
        
        # Verifica colunas obrigatórias
        missing_cols = [col for col in self.required_fields if col not in df.columns]
        if missing_cols:
            msg = f"Colunas obrigatórias faltando: {missing_cols}"
            validation_issues.append(msg)
            return False, validation_issues
        
        # Valida cada registro
        for idx, record in df.iterrows():
            try:
                EmployeeModel.model_validate(record.to_dict())
            except Exception as e:
                validation_issues.append(f"Linha {idx+1}: {str(e)}")
        
        # Verifica IDs duplicados
        if 'employee_id' in df.columns and df['employee_id'].duplicated().any():
            dups = df['employee_id'].duplicated().sum()
            validation_issues.append(f"IDs duplicados encontrados: {dups} casos")
        
        if not validation_issues:
            logger.info("Validação concluída sem erros")
            return True, []
        
        logger.warning(f"Validação encontrou {len(validation_issues)} problemas")
        return False, validation_issues
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e padroniza o DataFrame"""
        logger.info("Iniciando limpeza de dados")
        
        if df.empty:
            return df
        
        df_clean = df.copy()
        
        # Remove linhas completamente vazias
        df_clean = df_clean.dropna(how='all')
        
        # Padroniza campos string
        str_fields = [
            name for name, field in EmployeeModel.model_fields.items() 
            if field.annotation in (str, Optional[str])
        ]
        
        for field in str_fields:
            if field in df_clean.columns:
                df_clean[field] = (
                    df_clean[field]
                    .astype(str)
                    .str.strip()
                    .str.title()
                )
        
        # Padroniza e-mails
        if 'email' in df_clean.columns:
            df_clean['email'] = df_clean['email'].str.lower().str.strip()
        
        # Converte datas
        if 'hire_date' in df_clean.columns:
            df_clean['hire_date'] = pd.to_datetime(df_clean['hire_date'])
        
        # Adiciona metadados
        df_clean['processed_at'] = datetime.now()
        
        logger.info(f"Limpeza concluída. Registros: {len(df_clean)}")
        return df_clean

if __name__ == "__main__":
    # Configuração do logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Dados de teste
    test_data = pd.DataFrame({
        'employee_id': ['EMP001', 'EMP002', 'EMP003', 'INVALID'],
        'first_name': ['John', 'Jane', 'Bob', ' '],
        'last_name': ['Doe', 'Smith', 'Johnson', 'Missing'],
        'email': ['john@example.com', 'jane@example.com', 'invalid-email', None],
        'department': ['IT', 'HR', 'Finance', None],
        'position': ['Developer', 'Manager', 'Analyst', None],
        'hire_date': ['2020-01-15', '2021-02-28', '31/02/2023', 'not-a-date'],
        'data_source': ['HR', 'HR', 'HR', None],
        'phone': ['(11) 98765-4321', None, '1234567890', None]
    })
    
    print("\n=== DADOS DE TESTE ===")
    print(test_data)
    
    # Teste de validação
    validator = DataValidator()
    is_valid, issues = validator.validate_dataframe(test_data)
    
    print("\n=== RESULTADOS DA VALIDAÇÃO ===")
    print(f"Válido: {'Sim' if is_valid else 'Não'}")
    for issue in issues[:5]:  # Mostra no máximo 5 erros
        print(f"- {issue}")
    
    # Teste de limpeza
    if not is_valid:
        print("\n=== APLICANDO LIMPEZA ===")
        cleaned_data = validator.clean_dataframe(test_data)
        
        print("\nDados limpos:")
        print(cleaned_data)
        
        print("\n=== VALIDAÇÃO APÓS LIMPEZA ===")
        is_valid, issues = validator.validate_dataframe(cleaned_data)
        print(f"Válido após limpeza: {'Sim' if is_valid else 'Não'}")
        for issue in issues:
            print(f"- {issue}")