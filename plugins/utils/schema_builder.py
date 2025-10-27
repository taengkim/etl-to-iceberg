"""Iceberg 스키마를 생성하는 유틸리티"""
from typing import List, Dict, Any
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField
from utils.type_converter import oracle_to_iceberg_type


def create_iceberg_schema(
    oracle_columns: List[Dict[str, Any]], 
    oracle_schema: str,
    oracle_table: str,
    logger=None
) -> Schema:
    """
    Oracle 컬럼 정보로부터 Iceberg 스키마 생성
    
    :param oracle_columns: Oracle 컬럼 정보 리스트
    :param oracle_schema: Oracle 스키마명
    :param oracle_table: Oracle 테이블명
    :param logger: 로깅 객체 (선택사항)
    :return: Iceberg 스키마
    """
    iceberg_fields = []
    
    for col in oracle_columns:
        col_name = col['name'].lower()  # Iceberg는 소문자 권장
        col_type = oracle_to_iceberg_type(
            col['type'],
            col['nullable'],
            logger=logger
        )
        
        iceberg_fields.append(
            NestedField(
                field_id=None,  # Auto-assigned
                name=col_name,
                field_type=col_type,
                required=not col['nullable'],
                doc=f"From Oracle {oracle_schema}.{oracle_table}"
            )
        )
    
    return Schema(*iceberg_fields)
