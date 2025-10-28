"""
HDFS to Iceberg Operator

HDFS의 ORC 파일을 MinIO Iceberg (Parquet)로 이관하는 Operator입니다.
"""

import tempfile
import os
from typing import Optional, Dict, List
from collections import defaultdict
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from .hooks import HdfsHook
from utils.spark_builder import create_iceberg_spark_session


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
    :param catalog_name: Iceberg catalog 이름
    :param catalog_uri: Iceberg REST Catalog URI
    :param mode: 쓰기 모드 ('append', 'overwrite', 'upsert')
    :param partition_column: Partition 컬럼명 (예: 'dt', 'hour')
    :param partition_value: 특정 partition 값만 처리 (리스트 또는 단일 값)
    :param process_partitions_separately: 각 partition을 개별적으로 처리할지 여부
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
        catalog_name: str = 'iceberg',
        catalog_uri: str = 'http://iceberg-rest:8181',
        mode: str = 'append',
        partition_column: Optional[str] = None,
        partition_value: Optional[List[str]] = None,
        process_partitions_separately: bool = False,
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
        self.catalog_name = catalog_name
        self.catalog_uri = catalog_uri
        self.mode = mode
        self.partition_column = partition_column
        self.partition_value = partition_value or []
        if isinstance(partition_value, str):
            self.partition_value = [partition_value]
        self.process_partitions_separately = process_partitions_separately
        self.spark_config = spark_config or {}

    def _extract_partition_from_path(self, file_path: str) -> str:
        """
        파일 경로에서 partition 값 추출
        
        :param file_path: HDFS 파일 경로
        :return: partition 값
        """
        if not self.partition_column:
            return None
        
        # Hive-style partition 경로에서 값 추출
        # 예: /path/to/table/dt=2024-01-01/hour=10/file.orc
        parts = file_path.split('/')
        for part in parts:
            if '=' in part and part.startswith(f"{self.partition_column}="):
                return part.split('=')[1]
        
        return None
    
    def _group_files_by_partition(self, orc_files: List[str]) -> Dict[str, List[str]]:
        """
        ORC 파일을 partition별로 그룹화
        
        :param orc_files: ORC 파일 목록
        :return: partition 값별 파일 목록 딕셔너리
        """
        partition_files = defaultdict(list)
        
        for file_path in orc_files:
            partition_value = self._extract_partition_from_path(file_path)
            
            # partition 값이 없으면 'default'로 그룹화
            if not partition_value:
                partition_files['default'].append(file_path)
            else:
                # 특정 partition 값만 처리하도록 필터링
                if not self.partition_value or partition_value in self.partition_value:
                    partition_files[partition_value].append(file_path)
        
        return dict(partition_files)
    
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
            catalog_name = self.catalog_name or context.get('params', {}).get('catalog_name', 'iceberg')
            catalog_uri = self.catalog_uri or context.get('params', {}).get('catalog_uri')
            
            if not all([minio_endpoint, minio_access_key, minio_secret_key, minio_bucket]):
                raise AirflowException("MinIO 설정이 필요합니다.")
            
            if not catalog_uri:
                raise AirflowException("catalog_uri 설정이 필요합니다.")
            
            # ORC 파일 목록 로드
            orc_files = self._load_orc_files(hdfs_hook, self.hdfs_path)
            
            if not orc_files:
                self.log.warning("이관할 ORC 파일이 없습니다.")
                return
            
            self.log.info(f"총 {len(orc_files)}개의 ORC 파일 발견")
            
            # Partition별로 처리
            if self.partition_column:
                partition_files = self._group_files_by_partition(orc_files)
                self.log.info(f"{len(partition_files)}개의 partition 발견")
                
                for partition_val, files in partition_files.items():
                    self.log.info(f"Partition '{partition_val}': {len(files)}개 파일 처리")
                
                # 각 partition을 개별적으로 처리
                if self.process_partitions_separately:
                    for partition_val, files in partition_files.items():
                        self.log.info(f"Partition '{partition_val}' 처리 시작 ({len(files)}개 파일)")
                        self._transfer_with_pyspark(
                            files, hdfs_hook, minio_endpoint,
                            minio_access_key, minio_secret_key,
                            minio_bucket, warehouse_path, catalog_name, catalog_uri,
                            partition_value=partition_val if partition_val != 'default' else None
                        )
                else:
                    # 모든 partition을 한번에 처리
                    all_files = []
                    for files in partition_files.values():
                        all_files.extend(files)
                    self._transfer_with_pyspark(
                        all_files, hdfs_hook, minio_endpoint,
                        minio_access_key, minio_secret_key,
                        minio_bucket, warehouse_path, catalog_name, catalog_uri
                    )
            else:
                # Partition 없이 전체 처리
                self.log.info(f"총 {len(orc_files)}개의 ORC 파일 이관 시작")
                self._transfer_with_pyspark(
                    orc_files, hdfs_hook, minio_endpoint,
                    minio_access_key, minio_secret_key,
                    minio_bucket, warehouse_path, catalog_name, catalog_uri
                )
            
            self.log.info("HDFS to Iceberg 이관 완료")
        
        except Exception as e:
            self.log.error(f"HDFS to Iceberg 이관 실패: {e}")
            raise AirflowException(f"이관 실패: {e}")

    def _transfer_with_pyspark(self, orc_files, hdfs_hook, minio_endpoint, 
                               minio_access_key, minio_secret_key, 
                               minio_bucket, warehouse_path, catalog_name, catalog_uri,
                               partition_value=None):
        """PySpark를 사용한 이관"""
        try:
            from functools import reduce
            
            partition_suffix = f"_partition_{partition_value}" if partition_value else ""
            self.log.info(f"PySpark 세션 생성 중...{partition_suffix}")
            
            # SparkSession 생성
            spark = create_iceberg_spark_session(
                app_name=f'HDFS_TO_ICEBERG{partition_suffix}',
                catalog_name=catalog_name,
                catalog_uri=catalog_uri,
                warehouse_path=warehouse_path,
                additional_config=self.spark_config
            )
            
            self.log.info(f"HDFS ORC 파일 읽기 시작: {len(orc_files)}개")
            
            # Spark에서 여러 파일을 한번에 읽기
            # file:// 또는 hdfs:// 프로토콜이 없으면 기본값 설정
            spark_files = []
            for orc_path in orc_files:
                if orc_path.startswith('hdfs://') or orc_path.startswith('file://'):
                    spark_files.append(orc_path)
                else:
                    # hdfs:// 프로토콜 추가
                    spark_files.append(orc_path)
            
            # ORC 파일 읽기 (Spark가 여러 파일을 자동으로 병합)
            self.log.info(f"Spark에서 {len(spark_files)}개 파일 읽기")
            try:
                combined_df = spark.read.format("orc").load(spark_files)
            except Exception as e:
                # 실패 시 개별 파일로 읽기
                self.log.warning(f"일괄 읽기 실패, 개별 파일로 읽습니다: {e}")
                dfs = []
                for orc_path in orc_files:
                    try:
                        df = spark.read.format("orc").load(orc_path)
                        dfs.append(df)
                    except Exception as file_error:
                        self.log.error(f"파일 읽기 실패: {orc_path}, {file_error}")
                        continue
                
                if not dfs:
                    self.log.warning("읽을 수 있는 ORC 파일이 없습니다.")
                    spark.stop()
                    return
                
                # 데이터프레임 병합
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
    
