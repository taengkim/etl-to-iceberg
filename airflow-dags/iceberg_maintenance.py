"""
Iceberg Maintenance DAG

MinIO에 저장된 Iceberg 테이블의 유지보수를 수행하는 DAG입니다.
압축 및 오래된 스냅샷 삭제를 주 단위로 실행합니다.
"""

from airflow import DAG
from datetime import datetime, timedelta
from plugins.maintenance.iceberg_compaction_operator import IcebergCompactionOperator
from plugins.maintenance.iceberg_aging_operator import IcebergAgingOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'iceberg_maintenance',
    default_args=default_args,
    description='Iceberg maintenance using MinIO',
    schedule='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        'minio_endpoint': 'http://minio:9000',
        'minio_access_key': 'minioadmin',
        'minio_secret_key': 'minioadmin',
        'minio_bucket': 'iceberg',
        'warehouse_path': 's3://iceberg/warehouse',
    },
    tags=['iceberg', 'maintenance', 'minio'],
) as dag:
    
    # 1. 압축 작업
    compact = IcebergCompactionOperator(
        task_id='compact',
        iceberg_namespace='analytics',
        iceberg_table='transactions',
        file_size_mb=512,
    )
    
    # 2. 오래된 스냅샷 삭제
    age = IcebergAgingOperator(
        task_id='expire_snapshots',
        iceberg_namespace='analytics',
        iceberg_table='transactions',
        older_than_days=30,
        retain_last=10,
    )
    
    # 의존성 설정
    compact >> age

