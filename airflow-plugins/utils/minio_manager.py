"""MinIO를 사용한 Iceberg 테이블 관리 유틸리티"""
import os
from pyiceberg.catalog import load_catalog
from typing import Optional


def create_minio_catalog_config(
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_bucket: str,
    warehouse_path: Optional[str] = None
) -> dict:
    """
    MinIO 기반 Iceberg catalog 설정 생성
    
    :param minio_endpoint: MinIO endpoint (예: 'http://localhost:9000')
    :param minio_access_key: MinIO Access Key
    :param minio_secret_key: MinIO Secret Key
    :param minio_bucket: MinIO Bucket 이름
    :param warehouse_path: Warehouse 경로 (s3://bucket/path)
    :return: Catalog 설정 딕셔너리
    """
    # warehouse 경로 설정
    if not warehouse_path:
        warehouse_path = f"s3://{minio_bucket}/warehouse"
    
    catalog_config = {
        'type': 'rest',
        'uri': os.getenv('ICEBERG_REST_URI', 'http://localhost:8181'),
        'warehouse': warehouse_path,
        's3.endpoint': minio_endpoint,
        's3.access-key-id': minio_access_key,
        's3.secret-access-key': minio_secret_key,
        's3.path-style-access': 'true',  # MinIO는 path-style access 사용
    }
    
    # 인증 정보가 있으면 추가
    if os.getenv('ICEBERG_AUTH_TYPE'):
        catalog_config['auth-type'] = os.getenv('ICEBERG_AUTH_TYPE')
    if os.getenv('ICEBERG_CREDENTIAL'):
        catalog_config['credential'] = os.getenv('ICEBERG_CREDENTIAL')
    
    return catalog_config


def create_minio_catalog(
    catalog_name: str = 'minio_catalog',
    minio_endpoint: str = None,
    minio_access_key: str = None,
    minio_secret_key: str = None,
    minio_bucket: str = None,
    warehouse_path: Optional[str] = None
):
    """
    MinIO 기반 Iceberg catalog 생성 또는 가져오기
    
    :param catalog_name: Catalog 이름
    :param minio_endpoint: MinIO endpoint
    :param minio_access_key: MinIO Access Key
    :param minio_secret_key: MinIO Secret Key
    :param minio_bucket: MinIO Bucket 이름
    :param warehouse_path: Warehouse 경로
    :return: Catalog 객체
    """
    # 환경 변수에서 기본값 가져오기
    endpoint = minio_endpoint or os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    access_key = minio_access_key or os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = minio_secret_key or os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    bucket = minio_bucket or os.getenv('MINIO_BUCKET', 'iceberg')
    
    catalog_config = create_minio_catalog_config(
        minio_endpoint=endpoint,
        minio_access_key=access_key,
        minio_secret_key=secret_key,
        minio_bucket=bucket,
        warehouse_path=warehouse_path
    )
    
    return load_catalog(catalog_name, **catalog_config)


def get_iceberg_table(catalog, namespace: str, table_name: str, logger=None):
    """
    Iceberg 테이블 가져오기
    
    :param catalog: Catalog 객체
    :param namespace: Namespace (database)
    :param table_name: 테이블명
    :param logger: 로깅 객체 (선택사항)
    :return: Iceberg Table 객체
    """
    table_path = (namespace, table_name)
    try:
        table = catalog.load_table(table_path)
        if logger:
            logger.info(f"Loaded table: {namespace}.{table_name}")
        return table
    except Exception as e:
        if logger:
            logger.error(f"Failed to load table {namespace}.{table_name}: {e}")
        raise
