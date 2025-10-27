"""
Iceberg Snapshot Management DAG

Iceberg 테이블의 스냅샷을 관리하는 DAG입니다.
스냅샷 목록을 조회할 수 있습니다.
"""

from airflow import DAG
from datetime import datetime, timedelta
from plugins.maintenance.iceberg_snapshot_operator import IcebergSnapshotOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'iceberg_snapshot_management',
    default_args=default_args,
    description='Iceberg snapshot management',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        'minio_endpoint': 'http://minio:9000',
        'minio_access_key': 'minioadmin',
        'minio_secret_key': 'minioadmin',
        'minio_bucket': 'iceberg',
        'warehouse_path': 's3://iceberg/warehouse',
    },
    tags=['iceberg', 'snapshot'],
) as dag:
    
    list_snapshots = IcebergSnapshotOperator(
        task_id='list_snapshots',
        iceberg_namespace='analytics',
        iceberg_table='transactions',
        action='list',
    )

