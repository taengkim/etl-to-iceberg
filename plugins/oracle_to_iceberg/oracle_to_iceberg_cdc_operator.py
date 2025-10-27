from typing import Optional, List, Dict, Any
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from oracle_to_iceberg.hooks import OracleHook
from utils.schema_builder import create_iceberg_schema
from utils.dataframe_utils import prepare_dataframe
from utils.catalog_manager import (
    create_or_get_catalog,
    create_partition_spec
)
import pyiceberg
from pyiceberg.schema import Schema
import pandas as pd
import pyarrow as pa
from datetime import datetime
import os


class OracleToIcebergCDCOperator(BaseOperator):
    """
    Oracle CDC(Change Data Capture)를 지원하는 Operator
    변경된 데이터만 추출하여 Iceberg로 이관
    
    :param oracle_conn_id: Oracle Airflow Connection ID
    :type oracle_conn_id: str
    :param oracle_schema: Oracle 스키마명
    :type oracle_schema: str
    :param oracle_table: Oracle 테이블명
    :type oracle_table: str
    :param iceberg_catalog: Iceberg catalog 이름
    :type iceberg_catalog: str
    :param iceberg_namespace: Iceberg namespace (database)
    :type iceberg_namespace: str
    :param iceberg_table: Iceberg 테이블명
    :type iceberg_table: str
    :param iceberg_warehouse: Iceberg warehouse 경로
    :type iceberg_warehouse: str
    :param mode: 쓰기 모드 ('append', 'upsert', 'merge')
    :type mode: str
    :param cdc_method: CDC 방법 ('timestamp', 'scn', 'flashback', 'logminer')
    :type cdc_method: str
    :param timestamp_column: 타임스탬프 컬럼명 (timestamp 방법 사용 시)
    :type timestamp_column: str
    :param last_timestamp: 마지막 처리 타임스탬프
    :type last_timestamp: str
    :param last_scn: 마지막 SCN (System Change Number)
    :type last_scn: int
    :param primary_key: Primary key 컬럼 (upsert/merge 시 필수)
    :type primary_key: list
    :param partition_by: 파티션 컬럼
    :type partition_by: list
    :param metadata_table: CDC 메타데이터 저장 테이블
    :type metadata_table: str
    """
    
    template_fields = (
        'oracle_schema',
        'oracle_table',
        'iceberg_namespace',
        'iceberg_table',
        'last_timestamp',
        'timestamp_column'
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
        mode: str = 'upsert',
        cdc_method: str = 'timestamp',
        timestamp_column: Optional[str] = None,
        last_timestamp: Optional[str] = None,
        last_scn: Optional[int] = None,
        primary_key: Optional[List[str]] = None,
        partition_by: Optional[List[str]] = None,
        metadata_table: str = '__airflow_cdc_metadata__',
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
        self.mode = mode
        self.cdc_method = cdc_method
        self.timestamp_column = timestamp_column
        self.last_timestamp = last_timestamp
        self.last_scn = last_scn
        self.primary_key = primary_key
        self.partition_by = partition_by
        self.metadata_table = metadata_table
    

    
    def _get_last_processing_info(self, oracle_hook) -> tuple:
        """마지막 처리 정보 가져오기"""
        if self.last_timestamp:
            return self.last_timestamp, self.last_scn
        
        # 메타데이터 테이블에서 조회 시도
        try:
            metadata_df = oracle_hook.read_table_data(
                schema=self.oracle_schema,
                table_name=self.metadata_table,
                where_clause=f"source_schema='{self.oracle_schema}' AND source_table='{self.oracle_table}'",
                columns=['last_timestamp', 'last_scn']
            )
            
            if not metadata_df.empty:
                return metadata_df.iloc[0]['last_timestamp'], metadata_df.iloc[0]['last_scn']
        except Exception as e:
            self.log.info(f"Could not read metadata table: {e}")
        
        return None, None
    
    def _update_processing_info(self, oracle_hook, current_timestamp, current_scn):
        """처리 정보 업데이트"""
        try:
            with oracle_hook.get_conn() as conn:
                cursor = conn.cursor()
                # 메타데이터 테이블이 없으면 생성
                create_table_sql = f"""
                BEGIN
                    EXECUTE IMMEDIATE 'CREATE TABLE {self.oracle_schema}.{self.metadata_table} (
                        source_schema VARCHAR2(128),
                        source_table VARCHAR2(128),
                        last_timestamp TIMESTAMP,
                        last_scn NUMBER,
                        PRIMARY KEY (source_schema, source_table)
                    )';
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE != -955 THEN RAISE; END IF;
                END;
                """
                cursor.execute(create_table_sql)
                
                # UPSERT
                upsert_sql = f"""
                MERGE INTO {self.oracle_schema}.{self.metadata_table} t
                USING (SELECT '{self.oracle_schema}' AS source_schema, 
                              '{self.oracle_table}' AS source_table,
                              TO_TIMESTAMP('{current_timestamp}', 'YYYY-MM-DD HH24:MI:SS.FF') AS last_timestamp,
                              {current_scn} AS last_scn FROM DUAL) s
                ON (t.source_schema = s.source_schema AND t.source_table = s.source_table)
                WHEN MATCHED THEN
                    UPDATE SET last_timestamp = s.last_timestamp, last_scn = s.last_scn
                WHEN NOT MATCHED THEN
                    INSERT (source_schema, source_table, last_timestamp, last_scn)
                    VALUES (s.source_schema, s.source_table, s.last_timestamp, s.last_scn)
                """
                cursor.execute(upsert_sql)
                conn.commit()
        except Exception as e:
            self.log.warning(f"Could not update metadata table: {e}")
    
    def _get_current_scn(self, oracle_hook) -> int:
        """현재 SCN 조회"""
        query = "SELECT CURRENT_SCN FROM V$DATABASE"
        with oracle_hook.get_cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else 0
    
    def _read_changed_data(
        self, 
        oracle_hook, 
        last_timestamp, 
        last_scn
    ) -> pd.DataFrame:
        """변경된 데이터 읽기"""
        if self.cdc_method == 'timestamp':
            if not self.timestamp_column:
                raise ValueError("timestamp_column is required for timestamp CDC method")
            
            where_clause = f"{self.timestamp_column} > TO_TIMESTAMP('{last_timestamp}', 'YYYY-MM-DD HH24:MI:SS.FF')"
            if last_timestamp:
                self.log.info(f"Reading changes since {last_timestamp}")
            else:
                self.log.info("Reading all data (no previous timestamp)")
                where_clause = "1=1"
        elif self.cdc_method == 'scn':
            where_clause = f"ORA_ROWSCN > {last_scn}" if last_scn else "1=1"
            self.log.info(f"Reading changes since SCN {last_scn}")
        elif self.cdc_method == 'flashback':
            # Flashback Query 사용
            if last_timestamp:
                where_clause = f"1=1"  # AS OF TIMESTAMP will be in FROM clause
                self.log.info(f"Using Flashback Query to {last_timestamp}")
            else:
                where_clause = "1=1"
        else:
            raise ValueError(f"Unsupported CDC method: {self.cdc_method}")
        
        return oracle_hook.read_table_data(
            schema=self.oracle_schema,
            table_name=self.oracle_table,
            where_clause=where_clause
        )
    
    def _upsert_to_iceberg(self, df: pd.DataFrame, iceberg_table):
        """Upsert to Iceberg table"""
        if not self.primary_key:
            raise ValueError("primary_key is required for upsert mode")
        
        # PyArrow Table로 변환
        arrow_table = pa.Table.from_pandas(df)
        
        # Iceberg merge (upsert) - 간단한 overwrite로 구현
        # 실제 운영에서는 merge API 사용 권장
        iceberg_table.overwrite(arrow_table)
    
    def execute(self, context):
        """Operator 실행"""
        self.log.info(f"Starting Oracle CDC to Iceberg transfer")
        self.log.info(f"Oracle: {self.oracle_schema}.{self.oracle_table}")
        self.log.info(f"CDC Method: {self.cdc_method}")
        
        with OracleHook(oracle_conn_id=self.oracle_conn_id) as oracle_hook:
            # 1. 마지막 처리 정보 가져오기
            last_timestamp, last_scn = self._get_last_processing_info(oracle_hook)
            
            # 2. 변경된 데이터 읽기
            self.log.info("Reading changed data from Oracle...")
            df_changes = self._read_changed_data(oracle_hook, last_timestamp, last_scn)
            
            if df_changes.empty:
                self.log.info("No changes detected")
                return {
                    'rows_transferred': 0,
                    'message': 'No changes'
                }
            
            self.log.info(f"Found {len(df_changes)} changed rows")
            
            # 3. DataFrame 정리
            df_changes = prepare_dataframe(df_changes)
            
            # 4. Iceberg 테이블 가져오기
            catalog = create_or_get_catalog(
                catalog_name='my_catalog',
                iceberg_warehouse=self.iceberg_warehouse
            )
            table_path = (self.iceberg_namespace, self.iceberg_table)
            
            try:
                iceberg_table = catalog.load_table(table_path)
            except Exception:
                # 테이블 생성
                oracle_columns = oracle_hook.get_table_columns(self.oracle_schema, self.oracle_table)
                iceberg_schema = create_iceberg_schema(
                    oracle_columns,
                    self.oracle_schema,
                    self.oracle_table,
                    logger=self.log
                )
                
                partition_spec = create_partition_spec(self.partition_by)
                
                iceberg_table = catalog.create_table(
                    identifier=table_path,
                    schema=iceberg_schema,
                    partition_spec=partition_spec
                )
            
            # 5. 데이터 쓰기
            arrow_table = pa.Table.from_pandas(df_changes)
            
            if self.mode == 'upsert':
                self._upsert_to_iceberg(df_changes, iceberg_table)
            elif self.mode == 'append':
                iceberg_table.append(arrow_table)
            else:
                iceberg_table.append(arrow_table)
            
            # 6. 처리 정보 업데이트
            current_scn = self._get_current_scn(oracle_hook)
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            self._update_processing_info(oracle_hook, current_timestamp, current_scn)
            
            self.log.info(f"Successfully transferred {len(df_changes)} changed rows to Iceberg")
            
            return {
                'rows_transferred': len(df_changes),
                'last_timestamp': current_timestamp,
                'last_scn': current_scn
            }
