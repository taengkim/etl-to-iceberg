from typing import Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils.minio_manager import create_minio_catalog, get_iceberg_table
from datetime import datetime, timedelta
import os

# PySpark support
try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class IcebergAgingOperator(BaseOperator):
    """
    MinIO에 저장된 Iceberg 테이블의 aging을 수행하는 Operator
    
    오래된 스냅샷을 삭제하여 저장 공간을 확보합니다.
    
    :param iceberg_namespace: Iceberg namespace (database)
    :type iceberg_namespace: str
    :param iceberg_table: Iceberg 테이블명
    :type iceberg_table: str
    :param minio_endpoint: MinIO endpoint
    :type minio_endpoint: str
    :param minio_access_key: MinIO Access Key
    :type minio_access_key: str
    :param minio_secret_key: MinIO Secret Key
    :type minio_secret_key: str
    :param minio_bucket: MinIO Bucket 이름
    :type minio_bucket: str
    :param warehouse_path: Warehouse 경로 (선택사항)
    :type warehouse_path: str
    :param older_than_days: 이 값보다 오래된 스냅샷 삭제 (기본값: 7일)
    :type older_than_days: int
    :param retain_last: 최근 N개의 스냅샷은 항상 유지 (기본값: 10)
    :type retain_last: int
    :param use_pyspark: PySpark를 사용하여 aging 수행 (기본값: True)
    :type use_pyspark: bool
    """
    
    template_fields = (
        'iceberg_namespace',
        'iceberg_table',
        'minio_endpoint',
        'older_than_days',
        'retain_last'
    )
    
    @apply_defaults
    def __init__(
        self,
        iceberg_namespace: str,
        iceberg_table: str,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        minio_bucket: Optional[str] = None,
        warehouse_path: Optional[str] = None,
        older_than_days: int = 7,
        retain_last: int = 10,
        use_pyspark: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.iceberg_namespace = iceberg_namespace
        self.iceberg_table = iceberg_table
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.warehouse_path = warehouse_path
        self.older_than_days = older_than_days
        self.retain_last = retain_last
        self.use_pyspark = use_pyspark
    
    def execute(self, context):
        """Operator 실행"""
        self.log.info(f"Starting Iceberg Aging")
        self.log.info(f"Table: {self.iceberg_namespace}.{self.iceberg_table}")
        self.log.info(f"Older than: {self.older_than_days} days")
        self.log.info(f"Retain last: {self.retain_last} snapshots")
        
        # DAG params에서 MinIO 설정 가져오기
        minio_endpoint = self.minio_endpoint or context.get('params', {}).get('minio_endpoint')
        minio_access_key = self.minio_access_key or context.get('params', {}).get('minio_access_key')
        minio_secret_key = self.minio_secret_key or context.get('params', {}).get('minio_secret_key')
        minio_bucket = self.minio_bucket or context.get('params', {}).get('minio_bucket')
        warehouse_path = self.warehouse_path or context.get('params', {}).get('warehouse_path')
        
        # MinIO 설정을 인스턴스 변수에 저장
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.warehouse_path = warehouse_path
        
        try:
            if self.use_pyspark:
                if not PYSPARK_AVAILABLE:
                    raise ImportError("PySpark is not installed. Install it with: pip install pyspark")
                
                result = self._age_with_pyspark()
            else:
                # PyIceberg로 aging 수행
                result = self._age_with_pyiceberg()
            
            self.log.info(f"Aging completed successfully")
            return result
            
        except Exception as e:
            self.log.error(f"Aging failed: {e}")
            raise
    
    def _age_with_pyspark(self):
        """PySpark를 사용하여 aging 수행"""
        self.log.info("Using PySpark for aging")
        
        # SparkSession 생성
        spark = self._get_or_create_spark_session()
        
        # Iceberg expire snapshots SQL 실행
        table_name = f"{self.iceberg_namespace}.{self.iceberg_table}"
        
        aging_sql = f"""
        CALL spark_catalog.system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP('{self._get_expire_timestamp()}'),
            retain_last => {self.retain_last}
        )
        """
        
        self.log.info(f"Executing aging SQL: {aging_sql}")
        result = spark.sql(aging_sql)
        result.show()
        
        return {
            'action': 'age',
            'method': 'pyspark',
            'table': table_name,
            'older_than_days': self.older_than_days,
            'retain_last': self.retain_last
        }
    
    def _age_with_pyiceberg(self):
        """PyIceberg를 사용하여 aging 수행"""
        self.log.info("Using PyIceberg for aging")
        
        # MinIO catalog 생성
        catalog = create_minio_catalog(
            catalog_name='minio_catalog',
            minio_endpoint=self.minio_endpoint,
            minio_access_key=self.minio_access_key,
            minio_secret_key=self.minio_secret_key,
            minio_bucket=self.minio_bucket,
            warehouse_path=self.warehouse_path
        )
        
        # 테이블 가져오기
        table = get_iceberg_table(
            catalog,
            self.iceberg_namespace,
            self.iceberg_table,
            logger=self.log
        )
        
        # PyIceberg expire snapshots
        expire_timestamp = self._get_expire_timestamp()
        expired_files = table.expire_snapshots(
            older_than_timestamp_ms=self._timestamp_to_ms(expire_timestamp),
            retain_last=self.retain_last
        )
        
        self.log.info(f"Expired snapshots: {expired_files}")
        
        return {
            'action': 'age',
            'method': 'pyiceberg',
            'table': f"{self.iceberg_namespace}.{self.iceberg_table}",
            'older_than_days': self.older_than_days,
            'retain_last': self.retain_last,
            'expired_files': expired_files
        }
    
    def _get_expire_timestamp(self):
        """만료 타임스탬프 계산"""
        expire_date = datetime.now() - timedelta(days=self.older_than_days)
        return expire_date.strftime('%Y-%m-%d %H:%M:%S')
    
    def _timestamp_to_ms(self, timestamp_str: str) -> int:
        """타임스탬프 문자열을 밀리초로 변환"""
        dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        return int(dt.timestamp() * 1000)
    
    def _get_or_create_spark_session(self) -> SparkSession:
        """SparkSession 생성 또는 가져오기"""
        try:
            spark = SparkSession.getActiveSession()
            if spark is not None:
                return spark
        except:
            pass
        
        # MinIO 설정 가져오기
        endpoint = self.minio_endpoint or os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
        access_key = self.minio_access_key or os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        secret_key = self.minio_secret_key or os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        
        # 새 SparkSession 생성
        builder = SparkSession.builder.appName("IcebergAging")
        
        # 기본 설정
        builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
                        .config("spark.sql.catalog.spark_catalog.type", "hive") \
                        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # MinIO S3 설정
        builder = builder.config("spark.sql.catalog.spark_catalog.s3.endpoint", endpoint) \
                        .config("spark.sql.catalog.spark_catalog.s3.access-key-id", access_key) \
                        .config("spark.sql.catalog.spark_catalog.s3.secret-access-key", secret_key) \
                        .config("spark.sql.catalog.spark_catalog.s3.path-style-access", "true")
        
        # Warehouse 설정
        if self.warehouse_path:
            builder = builder.config("spark.sql.warehouse.dir", self.warehouse_path)
        
        spark = builder.getOrCreate()
        return spark
