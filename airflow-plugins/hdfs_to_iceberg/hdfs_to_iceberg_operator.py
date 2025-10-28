"""
HDFS to Iceberg Operator

HDFS의 ORC 파일을 MinIO Iceberg (Parquet)로 이관하는 Operator입니다.
"""

import tempfile
import os
from typing import Optional, Dict
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from .hooks import HdfsHook
from utils.minio_manager import create_minio_catalog, get_iceberg_table
from utils.function_converter import function_to_script


class HdfsToIcebergOperator(BaseOperator):
    """
    HDFS ORC 파일을 Iceberg Parquet 테이블로 이관하는 Operator
    
    :param hdfs_conn_id: HDFS 연결 ID
    :param hdfs_path: HDFS ORC 파일 경로 또는 디렉토리
    :param iceberg_namespace: Iceberg namespace
    :param iceberg_table: Iceberg 테이블명
    :param minio_endpoint: MinIO 엔드포인트
    :param minio_access_key: MinIO Access Key
    :param minio_secret_key: MinIO Secret Key
    :param minio_bucket: MinIO Bucket
    :param warehouse_path: Iceberg Warehouse 경로
    :param mode: 쓰기 모드 ('append', 'overwrite', 'upsert')
    :param use_pyspark: PySpark 사용 여부
    """

    template_fields = ('hdfs_path', 'iceberg_table', 'iceberg_namespace')

    @apply_defaults
    def __init__(
        self,
        hdfs_conn_id: str = 'hdfs_default',
        hdfs_path: str = None,
        iceberg_namespace: str = None,
        iceberg_table: str = None,
        minio_endpoint: str = None,
        minio_access_key: str = None,
        minio_secret_key: str = None,
        minio_bucket: str = None,
        warehouse_path: str = None,
        mode: str = 'append',
        use_pyspark: bool = True,
        spark_config: Optional[Dict[str, str]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_path = hdfs_path
        self.iceberg_namespace = iceberg_namespace
        self.iceberg_table = iceberg_table
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.warehouse_path = warehouse_path
        self.mode = mode
        self.use_pyspark = use_pyspark
        self.spark_config = spark_config or {}

    def _load_orc_files(self, hdfs_hook: HdfsHook, hdfs_path: str):
        """
        HDFS에서 ORC 파일 защиты 로드
        
        :param hdfs_hook: HDFS Hook
        :param hdfs_path: HDFS 경로
        :return: ORC 파일 목록
        """
        try:
            # 디렉토리인 경우 모든 ORC 파일 찾기
            if hdfs_hook.file_exists(hdfs_path):
                if hdfs_path.endswith('.orc'):
                    # 단일 파일
                    return [hdfs_path]
                else:
                    # 디렉토리에서 ORC 파일 찾기
                    files = hdfs_hook.list_files(hdfs_path, recursive=True)
                    orc_files = [f for f in files if f.endswith('.orc')]
                    self.log.info(f"ORC 파일 {len(orc_files)}개 발견: {hdfs_path}")
                    return orc_files
            else:
                raise AirflowException(f"HDFS 경로가 존재하지 않습니다: {hdfs_path}")
        
        except Exception as e:
            self.log.error(f"ORC 파일 로드 실패: {e}")
            raise AirflowException(f"ORC 파일 로드 실패: {e}")

    def execute(self, context: Dict) -> None:
        """
        HDFS ORC 파일을 Iceberg로 이관
        
        :param context: Airflow context
        """
        self.log.info(f"HDFS to Iceberg 이관 시작")
        self.log.info(f"Source: {self.hdfs_path}")
        self.log.info(f"Target: {self.iceberg_namespace}.{self.iceberg_table}")
        
        try:
            # HDFS Hook 초기화
            hdfs_hook = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
            
            # MinIO 설정 (DAG params 우선순위)
            minio_endpoint = self.minio_endpoint or context.get('params', {}).get('minio_endpoint')
            minio_access_key = self.minio_access_key or context.get('params', {}).get('minio_access_key')
            minio_secret_key = self.minio_secret_key or context.get('params', {}).get('minio_secret_key')
            minio_bucket = self.minio_bucket or context.get('params', {}).get('minio_bucket')
            warehouse_path = self.warehouse_path or context.get('params', {}).get('warehouse_path')
            
            if not all([minio_endpoint, minio_access_key, minio_secret_key, minio beneath_bucket]):
                raise AirflowException("MinIO 설정이 필요합니다.")
            
            # ORC 파일 목록 로드
            orc_files = self._load_orc_files(hdfs_hook, self.hdfs_path)
            
            if not orc_files:
                self.log.warning("이관할 ORC 파일이 없습니다.")
                return
            
            self.log.info(f"총 {len(orc_files)}개의 ORC 파일 이관 시작")
            
            # PySpark 사용 시
            if self.use_pyspark:
                self._transfer_with_pyspark(orc_files, hdfs_hook, minio_endpoint, 
                                          minio_access_key, minio_secret_key, 
                                          minio_bucket, warehouse_path)
            else:
                self._transfer_with_pyiceberg(orc_files, hdfs_hook, minio_endpoint,
                                             minio_access_key, minio_secret_key,
                                             minio_bucket, warehouse_path)
            
            self.log.info("HDFS to Iceberg 이관 완료")
        
        except Exception as e:
            self.log.error(f"HDFS to Iceberg 이관 실패: {e}")
            raise AirflowException(f"이관 실패: {e}")

    def _transfer_with_pyspark(self, orc_files, hdfs_hook, minio_endpoint, 
                               minio_access_key, minio_secret_key, 
                               minio_bucket, warehouse_path):
        """PySpark를 사용한 이관"""
        try:
            from pyspark.sql import SparkSession
            from functools import reduce
            
            self.log.info("PySpark 세션 생성 중...")
            
            # SparkSession 생성
            spark = SparkSession.builder \
                .appName("HDFS_TO_ICEBERG") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.iceberg.type", "rest") \
                .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
                .config("spark.sql.catalog.iceberg.warehouse", warehouse_path) \
                .config("spark.sql.parquet.compression.codec", "snappy") \
                .getOrCreate()
            
            self.log.info(f"HDFS ORC 파일 읽기 시작: {len(orc_files)}개")
            
            # HDFS ORC 파일 읽기
            dfs = []
            for orc_path in orc_files:
                self.log.info(f"ORC 파일 읽기: {orc_path}")
                df = spark.read.format("orc").load(orc_path)
                dfs.append(df)
            
            if not dfs:
                self.log.warning("읽을 수 있는 ORC 파일이 없습니다.")
                spark.stop()
                return
            
            # 모든 데이터프레임 합치기
            self.log.info("데이터프레임 병합 중...")
            combined_df = reduce(lambda df1, df2: df1.union(df2), dfs)
            
            # 데이터 크기 확인
            row_count = combined_df.count()
            self.log.info(f"총 {row_count}개의 레코드 처리")
            
            # Iceberg 테이블에 쓰기
            self.log.info(f"Iceberg 테이블에 쓰기: {self.iceberg_namespace}.{self.iceberg_table}")
            combined_df.write \
                .format("iceberg") \
                .mode(self.mode) \
                .saveAsTable(f"iceberg.{self.iceberg_namespace}.{self.iceberg_table}")
            
            self.log.info("PySpark 이관 완료")
            spark.stop()
        
        except Exception as e:
            self.log.error(f"PySpark 이관 실패: {e}")
            raise AirflowException(f"PySpark 이관 실패: {e}")
    
    def _transfer_with_pyiceberg(self, orc_files, hdfs_hook, minio_endpoint,
                                 minio_access_key, minio_secret_key,
                                 minio_bucket, warehouse_path):
        """PyIceberg를 사용한 이관"""
        from pyiceberg.catalog import load_catalog
        from pyiceberg.table import Table
        import pandas as pd
        
        # MinIO Iceberg Catalog 생성
        catalog = create_minio_catalog(
            catalog_name='minio_catalog',
            minio_endpoint=minio_endpoint,
            minio_access_key=minio_access_key,
            minio_secret_key=minio_secret_key,
            minio_bucket=minio_bucket,
            warehouse_path=warehouse_path
        )
        
        # 테이블 로드 또는 생성
        try:
            table = catalog.load_table(f"{self.iceberg_namespace}.{self.iceberg_table}")
        except:
            raise AirflowException(f"Iceberg 테이블이 존재하지 않습니다: {self.iceberg_namespace}.{self.iceberg_table}")
        
        self.log.info("PyIceberg를 사용한 이관 실행")
        self.log.info("Note: PyIceberg는 ORC 직접 변환을 지원하지 않으므로 PySpark 사용을 권장합니다.")
