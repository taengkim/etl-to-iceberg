"""Oracle 타입을 Iceberg 타입으로 변환하는 유틸리티"""
from typing import Optional, Any
from pyiceberg.types import *


def oracle_to_iceberg_type(oracle_type: str, nullable: bool, logger=None) -> Any:
    """
    Oracle 데이터 타입을 Iceberg 타입으로 변환
    
    :param oracle_type: Oracle 타입 (예: 'NUMBER', 'VARCHAR2')
    :param nullable: Nullable 여부
    :param logger: 로깅 객체 (선택사항)
    :return: Iceberg 타입
    """
    type_mapping = {
        'NUMBER': LongType(),
        'FLOAT': DoubleType(),
        'BINARY_FLOAT': FloatType(),
        'BINARY_DOUBLE': DoubleType(),
        'VARCHAR2': StringType(),
        'CHAR': StringType(),
        'NCHAR': StringType(),
        'NVARCHAR2': StringType(),
        'CLOB': StringType(),
        'NCLOB': StringType(),
        'DATE': TimestampType(),
        'TIMESTAMP': TimestampType(),
        'TIMESTAMP WITH TIME ZONE': TimestamptzType(),
        'RAW': BinaryType(),
        'BLOB': BinaryType(),
    }
    
    base_type = oracle_type.split('(')[0]
    iceberg_type = type_mapping.get(base_type)
    
    if iceberg_type is None and logger:
        logger.warning(f"Unmapped Oracle type: {oracle_type}, defaulting to StringType")
        iceberg_type = StringType()
    elif iceberg_type is None:
        iceberg_type = StringType()
    
    return iceberg_type
