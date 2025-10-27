from typing import Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils.minio_manager import create_minio_catalog, get_iceberg_table
import os

# PySpark support
try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class IcebergCompactionOperator(BaseOperator):
    """
    MinIO에 저장된 Iceberg 테이블의 compaction을 수행하는 Operator
    
    작은 데이터 파일들을 큰 파일로 병합하여 쿼리 성능을 향상시킵니다.
    
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
    :param file_size_mb: 목표 파일 크기 (MB, 기본값: 512)
    :type file_size_mb: int
    :param use_pyspark: PySpark를 사용하여 compaction 수행 (기본값: True)
    :type use_pyspark: bool
    """
    
    template_fields = (
        'iceberg_namespace',
        'iceberg_table',
        'minio_endpoint'
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
        file_size_mb: int = 512,
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
        self.file_size_mb = file_size_mb
        self.use_pyspark = use_pyspark
    
    def execute(self, context):
        """Operator 실행"""
        self.log.info(f"Starting Iceberg Compaction")
        self.log.info(f"Table: {self.iceberg_namespace}.{self.iceberg_table}")
        self.log.info(f"Target file size: {self.file_size_mb}MB")
        
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
                
                result = self._compact_with_pyspark()
            else:
                # PyIceberg로 compaction 수행
                result = self._compact_with_pyiceberg()
            
            self.log.info(f"Compaction completed successfully")
            return result
            
        except Exception as e:
            self.log.error(f"Compaction failed: {e}")
            raise
    
    def _compact_with_pyspark(self):
        """PySpark를 사용하여 compaction 수행"""
        self.log.info("Using PySpark for compaction")
        
        # SparkSession 생성
        spark = self._get_or_create_spark_session()
        
        # Iceberg compaction SQL 실행
        table_name = f"{self.iceberg_namespace}.{self.iceberg_table}"
        
        compaction_sql = f"""
        CALL spark_catalog.system.rewrite_data_files(
            table => '{table_name}',
            strategy => 'binpack',
            options => map('target-file-size-mb', '{self.file_size_mb}')
        )
        """
        
        self.log.info(f"Executing compaction SQL: {compaction_sql}")
        spark.sql(compaction_sql).show()
        
        return {
            'action': 'compact',
            'method': 'pyspark',
            'table': table_name,
            'file_size_mb': self.file_size_mb
        }
    
    def _compact_with_pyiceberg(self):
        """PyIceberg를 사용하여 compaction 수행"""
        self.log.info("Using PyIceberg for compaction")
        
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
        
        # PyIceberg에서 compaction은 수동으로 구현해야 함
        # 또는 Spark를 통해 수행
        self.log.info("PyIceberg compaction requires manual implementation or Spark")
        
        return {
            'action': 'compact',
            'method': 'pyiceberg',
            'table': f"{self.iceberg_namespace}.{self.iceberg_table}",
            'status': 'requires_spark'
        }
    
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
        builder = SparkSession.builder.appName("IcebergCompaction")
        
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
