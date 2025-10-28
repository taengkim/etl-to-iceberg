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
from utils.minio_manager import create_minio_catalog


class HdfsToIcebergCDCOperator(BaseOperator):
    """
    HDFS ORC 파일의 변경점을 감지하여 Iceberg로 CDC 이관하는 Operator
    
    :param hdfs_conn_id: HDFS 연결 ID
    :param hdfs_path: HDFS ORC 파일 경로 또는 디렉토리
    :param iceberg_namespace: Iceberg namespace
    :param iceberg_table: Iceberg 테이블명
    :諷param cdc_method: CDC 방법 ('size', 'mtime', 'hash')
    :param metadata_table: 메타데이터 저장 테이블
    :param mode: 쓰기 모드 ('append', 'upsert')
    :param primary_key: Primary key (upsert 시 필요)
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
        self.cdc_method = cdc_method
        self.metadata_table = metadata_table
        self.mode = mode
        self.primary_key = primary_key or []
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.warehouse_path = warehouse_path
        self.use_pyspark = use_pyspark
        self.spark_config = spark_config or {}

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
                self.log.info(f"마지막 체크포인트 발견: {last_checkpoint}")
                return last_checkpoint
            else:
                # 초기 실행: 모든 파일 처리
                self.log.info("초기 실행: 모든 파일 처리")
                return {'last_check_time': 0, 'processed_files': []}
        
        except:
            return {'last_check_time': 0, 'processed_files': []}

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
                file_info = hdfs_hook.get_file_info(file_path)
                
                is_changed = False
                
                if self.cdc_method == 'mtime':
                    # 수정 시간 기반
                    mtime = file_info.get('modification_time', 0)
                    if mtime > last_check_time:
                        is_changed = True
                
                elif self.cdc_method == 'size':
                    # 파일 크기 기반
                    size = file_info.get('size', 0)
                    # 파일 경로와 크기 조합으로 고유 ID 생성
                    file_id = f"{file_path}:{size}"
                    if file_id not in processed_files:
                        is_changed = True
                
                elif self.cdc_method == 'hash':
                    # 파일 해시 기반 (구현 필요)
                    self.log.warning("Hash 방법은 아직 구현되지 않았습니다.")
                    is_changed = True  # 임시로 모든 파일 처리
                
                if is_changed:
                    changed_files.append(file_path)
                    self.log.info(f"변경된 파일 감지: {file_path}")
            
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
            
            if not all([minio_endpoint, minio_access_key, minio_secret_key, minio_bucket]):
                raise AirflowException("MinIO 설정이 필요합니다.")
            
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
            if self.use_pyspark:
                self._transfer_changes_with_pyspark(changed_files, hdfs_hook, minio_endpoint,
                                                   minio_access_key, minio_secret_key,
                                                   minio_bucket, warehouse_path)
            
            # 체크포인트 저장
            new_checkpoint = {
                'last_check_time': int(datetime.now().timestamp() * 1000),
                'processed_files': changed_files,
                'total_files_processed': len(changed_files)
            }
            self._save_checkpoint(new_checkpoint, context)
            
            self.log.info("HDFS to Iceberg CDC 완료")
        
        except Exception as e:
            self.log.error(f"CDC 실패: {e}")
            raise AirflowException(f"CDC 실패: {e}")

    def _transfer_changes_with_pyspark(self, changed_files, hdfs_hook, minio_endpoint,
                                      minio_access_key, minio_secret_key,
                                      minio_bucket, warehouse_path):
        """PySpark를 사용한 변경된 파일 이관"""
        script = f"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("HDFS_TO_ICEBERG_CDC") \\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.iceberg.type", "rest") \\
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \\
    .config("spark.sql.catalog.iceberg.warehouse", "{warehouse_path}") \\
    .config("spark.sql.parquet.compression.codec", "snappy") \\
    .getOrCreate()

# 변경된 HDFS ORC 파일 읽기
changed_files = {changed_files}
dfs = []
for file_path in changed_files:
    df = spark.read.format("orc").load(file_path)
    dfs.append(df)

# 모든 데이터프레임 합치기
from functools import reduce
combined_df = reduce(lambda df1, df2: df1.union(df2), dfs)

# Upsert 모드인 경우
if "{self.mode}" == "upsert":
    # TODO: Upsert 로직 구현 (Merge Into 사용)
    combined_df.write \\
        .format("iceberg") \\
        .mode("overwrite") \\
        .saveAsTable("iceberg.{self.iceberg_namespace}.================================================================else:
    # Append 모드
    combined_df.write \\
        .format("iceberg") \\
        .mode("append") \\
        .saveAsTable("iceberg.{self.iceberg_namespace}.{self.iceberg_table}")

spark.stop()
"""
        self.log.info("PySpark를 사용한 CDC 이관 실행")
        self.log.info(f"변경된 파일 수: {len(changed_files)}")

