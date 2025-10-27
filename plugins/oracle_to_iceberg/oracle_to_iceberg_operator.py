from typing import Optional, List, Dict, Any
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from oracle_to_iceberg.hooks import OracleHook
from utils.schema_builder import create_iceberg_schema
from utils.dataframe_utils import prepare_dataframe
from utils.catalog_manager import (
    create_or_get_catalog,
    ensure_namespace_exists,
    create_partition_spec
)
import pyiceberg
from pyiceberg.schema import Schema
import pandas as pd
import pyarrow as pa
from datetime import datetime
import os

# PySpark support
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class OracleToIcebergOperator(BaseOperator):
    """
    Oracle 테이블 데이터를 Iceberg 테이블로 이관하는 Operator
    
    :param oracle_conn_id: Oracle Airflow Connection ID
    :type oracle_conn_id: str
    :param oracle_schema: Oracle 스키마명
    :type oracle_schema: str
    :param oracle_table: Oracle 테이블명
    :type oracle_table: str
    :param iceberg_catalog: Iceberg catalog 이름 (예: 'default')
    :type iceberg_catalog: str
    :param iceberg_namespace: Iceberg namespace (database)
    :type iceberg_namespace: str
    :param iceberg_table: Iceberg 테이블명 (없으면 oracle_table과 동일)
    :type iceberg_table: str
    :param iceberg_warehouse: Iceberg warehouse 경로
    :type iceberg_warehouse: str
    :param columns: 추출할 컬럼 목록 (None이면 전체)
    :type columns: list
    :param where_clause: WHERE 절 조건 (선택사항)
    :type where_clause: str
    :param chunksize: 청크 크기 (대용량 데이터 처리 시 사용)
    :type chunksize: int
    :param mode: 쓰기 모드 ('append', 'overwrite', 'merge')
    :type mode: str
    :param partition_by: 파티션 컬럼 (선택사항)
    :type partition_by: list
    :param write_engine: 쓰기 엔진 ('pyiceberg' 또는 'pyspark')
    :type write_engine: str
    :param spark_config: Spark 설정 딕셔너리
    :type spark_config: dict
    """
    
    template_fields = (
        'oracle_schema',
        'oracle_table',
        'iceberg_namespace',
        'iceberg_table',
        'where_clause'
    )
    
    @apply_defaults
    def __init__(
        self,
        oracle_conn_id: str = 'oracle_default',
        oracle_schema: str = None,
        oracle_table: str = None,
        iceberg_catalog: str = 'default',
        iceberg_namespace: str = 'default',
        iceberg_table: Optional[str] = None,
        iceberg_warehouse: Optional[str] = None,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        chunksize: Optional[int] = None,
        mode: str = 'append',
        partition_by: Optional[List[str]] = None,
        write_engine: str = 'pyiceberg',
        spark_config: Optional[Dict[str, str]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.oracle_schema = oracle_schema
        self.oracle_table = oracle_table
        self.iceberg_catalog = iceberg_catalog
        self.iceberg_namespace = iceberg_namespace
        self.iceberg_table = iceberg_table or oracle_table
        self.iceberg_warehouse = iceberg_warehouse
        self.columns = columns
        self.where_clause = where_clause
        self.chunksize = chunksize
        self.mode = mode
        self.partition_by = partition_by
        self.write_engine = write_engine
        self.spark_config = spark_config or {}
    

    
    def _write_dataframe_to_iceberg(
        self,
        df: pd.DataFrame,
        catalog=None,
        table=None,
        mode: str = 'append'
    ):
        """DataFrame을 Iceberg 테이블에 쓰기"""
        if self.write_engine == 'pyspark':
            self._write_with_pyspark(df, mode)
        else:
            # pyiceberg 방식
            arrow_table = pa.Table.from_pandas(df)
            if mode == 'append':
                table.append(arrow_table)
            elif mode == 'overwrite':
                table.overwrite(arrow_table)
            else:
                self.log.warning(f"Unknown mode: {mode}, using append")
                table.append(arrow_table)
    
    def _write_with_pyspark(self, df: pd.DataFrame, mode: str):
        """PySpark를 사용하여 DataFrame을 Iceberg 테이블에 쓰기"""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not installed. Install it with: pip install pyspark")
        
        self.log.info("Using PySpark to write to Iceberg")
        
        # SparkSession 생성
        spark = self._get_or_create_spark_session()
        
        # pandas DataFrame을 Spark DataFrame으로 변환
        spark_df = spark.createDataFrame(df)
        
        # Iceberg 테이블 경로
        warehouse = self.iceberg_warehouse or os.getenv('ICEBERG_WAREHOUSE', '/tmp/warehouse')
        table_path = f"{warehouse}/{self.iceberg_namespace}.{self.iceberg_table}"
        
        # Iceberg에 쓰기
        writer = spark_df.write \
            .format("iceberg") \
            .mode(mode)
        
        # 파티션 설정
        if self.partition_by:
            writer = writer.partitionBy(*[col.lower() for col in self.partition_by])
        
        writer.saveAsTable(f"{self.iceberg_namespace}.{self.iceberg_table}")
        
        self.log.info(f"Written {len(df)} rows to Iceberg using PySpark")
    
    def _get_or_create_spark_session(self) -> SparkSession:
        """SparkSession 생성 또는 가져오기"""
        try:
            # 기존 SparkSession이 있으면 가져오기
            spark = SparkSession.getActiveSession()
            if spark is not None:
                return spark
        except:
            pass
        
        # 새 SparkSession 생성
        builder = SparkSession.builder.appName("OracleToIceberg")
        
        # 기본 설정
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
                        .config("spark.sql.catalog.spark_catalog.type", "hive") \
                        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # 사용자 설정 추가
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)
        
        # Iceberg warehouse 설정
        warehouse = self.iceberg_warehouse or os.getenv('ICEBERG_WAREHOUSE', '/tmp/warehouse')
        builder = builder.config("spark.sql.warehouse.dir", warehouse)
        
        spark = builder.getOrCreate()
        return spark
    
    def execute(self, context):
        """Operator 실행"""
        self.log.info(f"Starting Oracle to Iceberg transfer")
        self.log.info(f"Oracle: {self.oracle_schema}.{self.oracle_table}")
        self.log.info(f"Iceberg: {self.iceberg_namespace}.{self.iceberg_table}")
        
        # 1. Oracle Hook 초기화 및 연결
        with OracleHook(oracle_conn_id=self.oracle_conn_id) as oracle_hook:
            # 2. Oracle 테이블 정보 가져오기
            self.log.info("Fetching Oracle table information...")
            oracle_columns = oracle_hook.get_table_columns(
                self.oracle_schema,
                self.oracle_table
            )
            table_count = oracle_hook.get_table_count(
                self.oracle_schema,
                self.oracle_table
            )
            
            self.log.info(f"Oracle table has {len(oracle_columns)} columns and {table_count} rows")
            
            # 3. Iceberg Catalog 및 Table 가져오기 또는 생성
            self.log.info("Connecting to Iceberg catalog...")
            
            # Iceberg catalog 생성 또는 가져오기
            catalog = create_or_get_catalog(
                catalog_name='my_catalog',
                iceberg_warehouse=self.iceberg_warehouse
            )
            
            # Namespace가 없으면 생성
            ensure_namespace_exists(catalog, self.iceberg_namespace, logger=self.log)
            
            # 테이블이 존재하는지 확인
            table_path = (self.iceberg_namespace, self.iceberg_table)
            
            try:
                iceberg_table = catalog.load_table(table_path)
                self.log.info(f"Using existing Iceberg table: {table_path}")
            except Exception:
                # 테이블이 없으면 생성
                self.log.info(f"Creating Iceberg table: {table_path}")
                iceberg_schema = create_iceberg_schema(
                    oracle_columns,
                    self.oracle_schema,
                    self.oracle_table,
                    logger=self.log
                )
                
                # 파티션 설정
                partition_spec = create_partition_spec(self.partition_by)
                
                iceberg_table = catalog.create_table(
                    identifier=table_path,
                    schema=iceberg_schema,
                    partition_spec=partition_spec
                )
            
            # 4. Oracle 데이터 읽기 및 Iceberg에 쓰기
            self.log.info("Reading data from Oracle...")
            
            total_rows = 0
            
            if self.chunksize:
                # 청크 단위로 처리
                chunk_generator = oracle_hook.read_table_data(
                    schema=self.oracle_schema,
                    table_name=self.oracle_table,
                    columns=self.columns,
                    chunksize=self.chunksize,
                    where_clause=self.where_clause
                )
                
                for chunk_idx, chunk_df in enumerate(chunk_generator):
                    self.log.info(f"Processing chunk {chunk_idx + 1}, rows: {len(chunk_df)}")
                    
                    # 데이터 타입 변환 및 정리
                    chunk_df = prepare_dataframe(chunk_df)
                    
                    # Iceberg에 쓰기 (첫 청크만 overwrite, 나머지는 append)
                    if chunk_idx == 0 and self.mode == 'overwrite':
                        self._write_dataframe_to_iceberg(
                            chunk_df,
                            catalog,
                            iceberg_table,
                            mode='overwrite'
                        )
                    else:
                        self._write_dataframe_to_iceberg(
                            chunk_df,
                            catalog,
                            iceberg_table,
                            mode='append'
                        )
                    
                    total_rows += len(chunk_df)
            else:
                # 전체 데이터 한번에 처리
                df = oracle_hook.read_table_data(
                    schema=self.oracle_schema,
                    table_name=self.oracle_table,
                    columns=self.columns,
                    where_clause=self.where_clause
                )
                
                self.log.info(f"Read {len(df)} rows from Oracle")
                
                # 데이터 타입 변환 및 정리
                df = prepare_dataframe(df)
                
                # Iceberg에 쓰기
                self._write_dataframe_to_iceberg(
                    df,
                    catalog,
                    iceberg_table,
                    mode=self.mode
                )
                
                total_rows = len(df)
            
            self.log.info(f"Successfully transferred {total_rows} rows to Iceberg")
            
            return {
                'oracle_schema': self.oracle_schema,
                'oracle_table': self.oracle_table,
                'iceberg_namespace': self.iceberg_namespace,
                'iceberg_table': self.iceberg_table,
                'rows_transferred': total_rows
            }
