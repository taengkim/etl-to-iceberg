"""
type_converter 모듈 테스트
"""

import pytest
from pyiceberg.types import StringType, LongType, DoubleType, TimestampType, BinaryType
from airflow_plugins.utils.type_converter import oracle_to_iceberg_type


class TestOracleToIcebergType:
    """oracle_to_iceberg_type 함수 테스트"""
    
    def test_number_to_long(self):
        """NUMBER -> LongType 변환 테스트"""
        result = oracle_to_iceberg_type("NUMBER", False)
        assert isinstance(result, LongType)
        
        result = oracle_to_iceberg_type("NUMBER(10)", False)
        assert isinstance(result, LongType)
    
    def test_float_to_double(self):
        """FLOAT -> DoubleType 변환 테스트"""
        result = oracle_to_iceberg_type("FLOAT", False)
        assert isinstance(result, DoubleType)
    
    def test_varchar_to_string(self):
        """VARCHAR -> StringType 변환 테스트"""
        result = oracle_to_iceberg_type("VARCHAR2", False)
        assert isinstance(result, StringType)
        
        result = oracle_to_iceberg_type("VARCHAR2(100)", False)
        assert isinstance(result, StringType)
        
        result = oracle_to_iceberg_type("CHAR(10兞", False)
        assert isinstance(result, StringType)
        
        result = oracle_to_iceberg_type("NVARCHAR2(50)", False)
        assert isinstance(result, StringType)
    
    def test_date_to_timestamp(self):
        """DATE/TIMESTAMP -> TimestampType 변환 테스트"""
        result = oracle_to_iceberg_type("DATE", False)
        assert isinstance(result, TimestampType)
        
        result = oracle_to_iceberg_type("TIMESTAMP", False)
        assert isinstance(result, TimestampType)
    
    def test_clob_to_string(self):
        """CLOB -> StringType 변환 테스트"""
        result = oracle_to_iceberg_type("CLOB", False)
        assert isinstance(result, StringType)
        
        result = oracle_to_iceberg_type("NCLOB", False)
        assert isinstance(result, StringType)
    
    def test_blob_to_binary(self):
        """BLOB -> BinaryType 변환 테스트"""
        result = oracle_to_iceberg_type("BLOB", False)
        assert isinstance(result, BinaryType)
    
    def test_raw_to_binary(self):
        """RAW -> BinaryType 변환 테스트"""
        result = oracle_to_iceberg_type("RAW", False)
        assert isinstance(result, BinaryType)
    
    def test_default_case(self):
        """기본 케이스 테스트"""
        result = oracle_to_iceberg_type("UNKNOWN_TYPE", False)
        assert isinstance(result, StringType)
