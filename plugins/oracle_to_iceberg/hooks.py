from typing import Optional, List, Dict, Any
from airflow.hooks.base import BaseHook
import cx_Oracle
import pandas as pd
from contextlib import contextmanager


class OracleHook(BaseHook):
    """Oracle 데이터베이스 연결을 위한 Hook"""
    
    def __init__(
        self,
        oracle_conn_id: str = 'oracle_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.connection = None
        
    def get_conn(self):
        """Oracle 연결 생성"""
        if self.connection is None:
            conn = self.get_connection(self.oracle_conn_id)
            
            # Oracle 연결 문자열 생성
            # conn.extra에서 추가 설정을 가져올 수 있음
            dsn = cx_Oracle.makedsn(
                host=conn.host,
                port=conn.port or 1521,
                service_name=conn.schema
            )
            
            self.connection = cx_Oracle.connect(
                user=conn.login,
                password=conn.password,
                dsn=dsn
            )
            
        return self.connection
    
    @contextmanager
    def get_cursor(self):
        """커서 컨텍스트 매니저"""
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> List[tuple]:
        """쿼리 실행 및 결과 반환"""
        with self.get_cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
    
    def execute_query_to_dataframe(self, sql: str, params: Optional[List] = None) -> pd.DataFrame:
        """쿼리 실행 결과를 pandas DataFrame으로 반환"""
        with self.get_cursor() as cursor:
            if params:
                return pd.read_sql(sql, con=self.get_conn(), params=params)
            else:
                return pd.read_sql(sql, con=self.get_conn())
    
    def get_table_count(self, schema: str, table_name: str) -> int:
        """테이블의 행 수를 반환"""
        sql = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        result = self.execute_query(sql)
        return result[0][0] if result else 0
    
    def get_table_columns(self, schema: str, table_name: str) -> List[Dict[str, Any]]:
        """테이블의 컬럼 정보를 반환"""
        sql = f"""
        SELECT 
            column_name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable
        FROM all_tab_columns
        WHERE owner = UPPER('{schema}')
        AND table_name = UPPER('{table_name}')
        ORDER BY column_id
        """
        results = self.execute_query(sql)
        columns = []
        for row in results:
            columns.append({
                'name': row[0],
                'type': row[1],
                'length': row[2],
                'precision': row[3],
                'scale': row[4],
                'nullable': row[5] == 'Y'
            })
        return columns
    
    def read_table_data(
        self,
        schema: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        chunksize: Optional[int] = None,
        where_clause: Optional[str] = None
    ):
        """테이블 데이터를 pandas DataFrame으로 읽기 (청크 단위 가능)"""
        if columns:
            cols = ', '.join(columns)
        else:
            cols = '*'
        
        sql = f"SELECT {cols} FROM {schema}.{table_name}"
        
        if where_clause:
            sql += f" WHERE {where_clause}"
        
        self.log.info(f"Executing query: {sql}")
        
        if chunksize:
            # 청크 단위로 데이터 읽기
            return pd.read_sql(
                sql,
                con=self.get_conn(),
                chunksize=chunksize
            )
        else:
            # 전체 데이터 읽기
            return pd.read_sql(sql, con=self.get_conn())
    
    def close(self):
        """연결 종료"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
