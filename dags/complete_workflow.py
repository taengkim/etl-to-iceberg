"""
Complete Workflow DAG

Oracle에서 Iceberg로 데이터 이관 후 압축 및 오래된 스냅샷을 삭제하는 전체 워크플로우 DAG입니다.
"""

from airflow import DAG
from datetime import datetime, timedelta
from plugins.oracle_to_iceberg.oracle_to_iceberg_operator import OracleToIcebergOperator
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
    'complete_workflow',
    default_args=default_args,
    description='Complete workflow: Oracle transfer + compaction + aging',
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
    tags=['oracle', 'iceberg', 'maintenance', 'complete'],
) as dag:
    
    # Task 1: Oracle에서 Iceberg로 이관
    transfer_complete = OracleToIcebergOperator(
        task_id='transfer_complete',
        oracle_conn_id='oracle_default',
        oracle_schema='ANALYTICS',
        oracle_table='DAILY_METRICS',
        iceberg_namespace='analytics',
        iceberg_table='daily_metrics',
        iceberg_warehouse='s3://iceberg/warehouse',
        mode='append',
    )
    
    # Task 2: 압축
    compact_complete = IcebergCompactionOperator(
        task_id='compact_complete',
        iceberg_namespace='analytics',
        iceberg_table='daily_metrics',
        file_size_mb=512,
    )
    
    # Task 3: 오래된 스냅샷 삭제
    age_complete = IcebergAgingOperator(
        task_id='age_complete',
        iceberg_namespace='analytics',
        iceberg_table='daily_metrics',
        older_than_days=7,
        retain_last=10,
    )
    
    # 의존성 설정
    transfer_complete >> compact_complete >> age_complete

