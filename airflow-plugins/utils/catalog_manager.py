"""Iceberg Catalog 관리 유틸리티"""
import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField


def get_catalog_config(iceberg_warehouse: str = None) -> dict:
    """
    Iceberg catalog 설정 생성
    
    :param iceberg_warehouse: Iceberg warehouse 경로
    :return: Catalog 설정 딕셔너리
    """
    catalog_config = {
        'type': 'rest',
        'uri': os.getenv('ICEBERG_REST_URI', 'http://localhost:8181'),
        'warehouse': iceberg_warehouse or os.getenv('ICEBERG_WAREHOUSE', '/tmp/warehouse')
    }
    
    if os.getenv('ICEBERG_AUTH_TYPE'):
        catalog_config['auth-type'] = os.getenv('ICEBERG_AUTH_TYPE')
    if os.getenv('ICEBERG_CREDENTIAL'):
        catalog_config['credential'] = os.getenv('ICEBERG_CREDENTIAL')
    
    return catalog_config


def create_or_get_catalog(catalog_name: str = 'my_catalog', iceberg_warehouse: str = None):
    """
    Iceberg catalog 생성 또는 가져오기
    
    :param catalog_name: Catalog 이름
    :param iceberg_warehouse: Iceberg warehouse 경로
    :return: Catalog 객체
    """
    catalog_config = get_catalog_config(iceberg_warehouse)
    return load_catalog(catalog_name, **catalog_config)


def ensure_namespace_exists(catalog, namespace: str, logger=None):
    """
    Namespace가 존재하는지 확인하고 없으면 생성
    
    :param catalog: Catalog 객체
    :param namespace: Namespace 이름
    :param logger: 로깅 객체 (선택사항)
    """
    try:
        catalog.list_namespaces(namespace)
    except Exception:
        if logger:
            logger.info(f"Creating namespace: {namespace}")
        catalog.create_namespace(namespace)


def create_partition_spec(partition_by: list):
    """
    파티션 스펙 생성
    
    :param partition_by: 파티션 컬럼 리스트
    :return: 파티션 스펙 또는 None
    """
    if not partition_by:
        return None
    
    return PartitionSpec(
        *[PartitionField(source_name=col) for col in partition_by]
    )
