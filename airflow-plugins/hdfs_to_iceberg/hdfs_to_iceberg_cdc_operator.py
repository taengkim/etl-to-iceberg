"""
HDFS to Iceberg CDC Operator

HDFS ORC 파일의 변경점을 감지하여 Iceberg로 CDC 이관하는 Operator입니다.
파일 크기 및 수정 시간을 기준으로 변경점을 추적합니다.
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, List
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from .hooks import HdfsHook
from utils.spark_builder import create_iceberg_spark_session
from functools import reduce
from collections import defaultdict


class HdfsToIcebergCDCOperator(BaseOperator):
    """
    HDFS ORC 파일의 변경점을 감지하여 Iceberg로 CDC 이관하는 Operator
    
    :param hdfs_conn_id: HDFS 연결 ID
    :param hdfs_path: HDFS ORC 파일 경로 또는 디렉토리
    :param iceberg_namespace: Iceberg namespace
    :param iceberg_table: Iceberg 테이블명
    :param cdc_method: CDC 방법 ('size', 'mtime', 'hash')
    :param metadata_table: 메타데이터 저장 테이블
    :param mode: 쓰기 모드 ('append', 'upsert')
    :param primary_key: Primary key (upsert 시 필요)
    :param catalog_name: Iceberg catalog 이름
    :param catalog_uri: Iceberg REST Catalog URI
    """

    template_fields = ('hdfs_path', 'iceberg_table', 'iceberg_namespace', 'metadata_table')

    @apply_defaults
    def __init__(
        self,
        hdfs_conn_id: str = 'hdfs_default',
        hdfs_path: str = None,
        iceberg_namespace: str = None,
        iceberg_table: str = None,
        cdc_method: str = 'mtime',  # mtime (modification time), size, hash
        metadata_table: str = '__airflow_cdc_metadata__',
        mode: str = 'append',
        primary_key: Optional[List[str]] = None,
        minio_endpoint: str = None,
        minio_access_key: str = None,
        minio_secret_key: str = None,
        minio_bucket: str = None,
        warehouse_path: str = None,
        catalog_name: str = 'iceberg',
        catalog_uri: str = 'http://iceberg-rest:8181',
        spark_config: Optional[Dict[str, str]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_path = hdfs_path
        self.iceberg_namespace = iceberg_namespace
        self.iceberg_table = iceberg_table
        self.cdc_method = cdc_method
        self.metadata_table = metadata_table
        self.mode = mode
        self.primary_key = primary_key or []
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.warehouse_path = warehouse_path
        self.catalog_name = catalog_name
        self.catalog_uri = catalog_uri
        self.spark_config = spark_config or {}
    
    def _extract_partition_from_path(self, file_path: str, partition_prefix: str = 'dt') -> str:
        """
        파일 경로에서 partition 값 추출
        
        :param file_path: HDFS 파일 경로
        :param partition_prefix: Partition 컬럼명 (예: 'dt')
        :return: partition 값
        """
        # Hive-style partition 경로에서 값 추출
        # 예: /path/to/table/dt=2024-01-01/hour=10/file.orc
        parts = file_path.split('/')
        for part in parts:
            if '=' in part and part.startswith(f"{partition_prefix}="):
                return part.split('=')[1]
        
        return None
    
    def _group_files_by_partition(self, files: List[str], partition_prefix: str = 'dt') -> Dict[str, List[str]]:
        """
        파일 목록을 partition별로 그룹화
        
        :param files: 파일 경로 목록
        :param partition_prefix: Partition 컬럼명
        :return: partition 값별 파일 목록
        """
        partition_files = defaultdict(list)
        
        for file_path in files:
            partition_value = self._extract_partition_from_path(file_path, partition_prefix)
            if partition_value:
                partition_files[partition_value].append(file_path)
            else:
                partition_files['default'].append(file_path)
        
        return dict(partition_files)

    def _get_last_checkpoint(self, hdfs_hook: HdfsHook, context: Dict) -> Dict:
        """
        마지막 체크포인트 정보 가져오기
        
        :param hdfs_hook: HDFS Hook
        :param context: Airflow context
        :return: 마지막 체크포인트 정보
        """
        # XCom에서 마지막 체크포인트 가져오기
        ti = context.get('ti')
        
        try:
            last_checkpoint = ti.xcom_pull(
                task_ids=self.task_id,
                key='last_checkpoint'
            )
            
            if last_checkpoint:
                self.log.info(f"마지막 체크포인트 발견")
                return last_checkpoint
            else:
                # 초기 실행: 모든 파일 처리
                self.log.info("초기 실행: 모든 파일 처리")
                return {
                    'last_check_time': 0, 
                    'processed_files': [],
                    'partitions': {}
                }
        
        except:
            return {
                'last_check_time': 0, 
                'processed_files': [],
                'partitions': {}
            }

    def _save_checkpoint(self, checkpoint: Dict, context: Dict):
        """
        체크포인트 저장
        
        :param checkpoint: 체크포인트 정보
        :param context: Airflow context
        """
        ti = context.get('ti')
        
        try:
            ti.xcom_push(
                key='last_checkpoint',
                value=checkpoint
            )
            self.log.info(f"체크포인트 저장: {checkpoint}")
        
        except Exception as e:
            self.log.warning(f"체크포인트 저장 실패: {e}")

    def _detect_changes(self, hdfs_hook: HdfsHook, hdfs_path: str, 
                       last_checkpoint: Dict) -> List[str]:
        """
        HDFS 파일 변경점 감지
        
        :param hdfs_hook: HDFS Hook
        :param hdfs_path: HDFS 경로
        :param last_checkpoint: 마지막 체크포인트
        :return: 변경된 파일 목록
        """
        changed_files = []
        last_check_time = last_checkpoint.get('last_check_time', 0)
        processed_files = set(last_checkpoint.get('processed_files', []))
        partition_checkpoints = last_checkpoint.get('partitions', {})
        
        try:
            # ORC 파일 목록 가져오기
            if hdfs_hook.file_exists(hdfs_path):
                if hdfs_path.endswith('.orc'):
                    files = [hdfs_path]
                else:
                    files = hdfs_hook.list_files(hdfs_path, recursive=True)
                    files = [f for f in files if f.endswith('.orc')]
            else:
                raise AirflowException(f"HDFS 경로가 존재하지 않습니다: {hdfs_path}")
            
            self.log.info(f"총 {len(files)}개의 ORC 파일 검사")
            
            # 각 파일의 변경점 감지
            for file_path in files:
                partition_val = self._extract_partition_from_path(file_path)
                
                # Partition별 체크포인트 사용
                if partition_val and partition_val in partition_checkpoints:
                    partition_checkpoint = partition_checkpoints[partition_val]
                    partition_last_check = partition_checkpoint.get('last_check_time', 0)
                    partition_processed_files = set(partition_checkpoint.get('files', []))
                else:
                    partition_last_check = last_check_time
                    partition_processed_files = processed_files
                
                file_info = hdfs_hook.get_file_info(file_path)
                
                is_changed = False
                
                if self.cdc_method == 'mtime':
                    # 수정 시간 기반 (partition별 마지막 체크 시간 사용)
                    mtime = file_info.get('modification_time', 0)
                    if mtime > partition_last_check:
                        is_changed = True
                
                elif self.cdc_method == 'size':
                    # 파일 크기 기반
                    size = file_info.get('size', 0)
                    file_id = f"{file_path}:{size}"
                    if file_id not in partition_processed_files:
                        is_changed = True
                
                elif self.cdc_method == 'hash':
                    self.log.warning("Hash 방법은 아직 구현되지 않았습니다.")
                    is_changed = True
                
                if is_changed:
                    changed_files.append(file_path)
                    partition_log = f" [{partition_val}]" if partition_val else ""
                    self.log.info(f"변경된 파일 감지{partition_log}: {file_path}")
            
            self.log.info(f"총 {len(changed_files)}개의 변경된 파일 발견")
            return changed_files
        
        except Exception as e:
            self.log.error(f"변경점 감지 실패: {e}")
            raise AirflowException(f"변경점 감지 실패: {e}")

    def execute(self, context: Dict) -> None:
        """
        HDFS ORC 파일의 변경점을 감지하여 Iceberg로 CDC 이관
        
        :param context: Airflow context
        """
        self.log.info(f"HDFS to Iceberg CDC 시작")
        self.log.info(f"Source: {self.hdfs_path}")
        self.log.info(f"Target: {self.iceberg_namespace}.{self.iceberg_table}")
        self.log.info(f"CDC Method: {self.cdc_method}")
        
        try:
            # HDFS Hook 초기화
            hdfs_hook = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
            
            # MinIO 설정
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
            
            # 마지막 체크포인트 가져오기
            last_checkpoint = self._get_last_checkpoint(hdfs_hook, context)
            
            # 변경된 파일 감지
            changed_files = self._detect_changes(hdfs_hook, self.hdfs_path, last_checkpoint)
            
            if not changed_files:
                self.log.info("변경된 파일이 없습니다.")
                return
            
            # 변경된 파일 이관
            self.log.info(f"변경된 {len(changed_files)}개의 파일 이관 시작")
            
            # PySpark를 사용한 이관
            self._transfer_changes_with_pyspark(changed_files, hdfs_hook, minio_endpoint,
                                               minio_access_key, minio_secret_key,
                                               minio_bucket, warehouse_path, catalog_name, catalog_uri)
            
            # 체크포인트 저장 (partition별 정보 포함)
            partition_files = self._group_files_by_partition(changed_files)
            current_time = int(datetime.now().timestamp() * 1000)
            
            # 기존 partition 체크포인트 가져오기
            existing_partitions = last_checkpoint.get('partitions', {})
            
            # 변경된 partition의 체크포인트 업데이트
            for partition_val, files in partition_files.items():
                if partition_val not in existing_partitions:
                    existing_partitions[partition_val] = {'files': [], 'last_check_time': 0}
                
                existing_partitions[partition_val]['files'] = files
                existing_partitions[partition_val]['last_check_time'] = current_time
                self.log.info(f"Partition '{partition_val}' 체크포인트 업데이트: {len(files)}개 파일")
            
            new_checkpoint = {
                'last_check_time': current_time,
                'processed_files': changed_files,
                'total_files_processed': len(changed_files),
                'partitions': existing_partitions
            }
            self._save_checkpoint(new_checkpoint, context)
            
            self.log.info("HDFS to Iceberg CDC 완료")
        
        except Exception as e:
            self.log.error(f"CDC 실패: {e}")
            raise AirflowException(f"CDC 실패: {e}")

    def _transfer_changes_with_pyspark(self, changed_files, hdfs_hook, minio_endpoint,
                                      minio_access_key, minio_secret_key,
                                      minio_bucket, warehouse_path, catalog_name, catalog_uri):
        """PySpark를 사용한 변경된 파일 이관"""
        try:
            self.log.info("PySpark를 사용한 CDC 이관 실행")
            self.log.info(f"변경된 파일 수: {len(changed_files)}")
            
            # SparkSession 생성
            spark = create_iceberg_spark_session(
                app_name='HDFS_TO_ICEBERG_CDC',
                catalog_name=catalog_name,
                catalog_uri=catalog_uri,
                warehouse_path=warehouse_path,
                additional_config=self.spark_config
            )
            
            self.log.info("HDFS ORC 파일 읽기 시작")
            
            # 변경된 HDFS ORC 파일 읽기
            dfs = []
            for orc_path in changed_files:
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
            
            # Upsert 모드인 경우
            if self.mode == "upsert":
                # TODO: Upsert 로직 구현 (Merge Into 사용)
                self.log.info("Upsert 모드로 쓰기 (현재는 overwrite로 처리)")
                combined_df.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .saveAsTable(f"iceberg.{self.iceberg_namespace}.{self.iceberg_table}")
            else:
                # Append 모드
                self.log.info(f"Append 모드로 쓰기: {self.iceberg_namespace}.{self.iceberg_table}")
                combined_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(f"iceberg.{self.iceberg_namespace}.{self.iceberg_table}")
            
            self.log.info("CDC 이관 완료")
            spark.stop()
        
        except Exception as e:
            self.log.error(f"PySpark CDC 이관 실패: {e}")
            raise AirflowException(f"CDC 이관 실패: {e}")

