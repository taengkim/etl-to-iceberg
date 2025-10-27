"""
Oracle to Iceberg Partitioned Table DAG

파티션된 테이블을 Oracle에서 Iceberg로 이관하는 DAG입니다.
"""

from airflow import DAG
from datetime import datetime, timedelta
from plugins.oracle_to_iceberg.oracle_to_iceberg_operator import OracleToIcebergOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'oracle_to_iceberg_partitioned',
    default_args=default_args,
    description='Partitioned table transfer',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oracle', 'iceberg', 'partitioned'],
) as dag:
    
    transfer_sales_history = OracleToIcebergOperator(
        task_id='transfer_sales_history',
        oracle_conn_id='oracle_default',
        oracle_schema='SALES',
        oracle_table='SALES_HISTORY',
        iceberg_namespace='analytics',
        iceberg_table='sales_history',
        iceberg_warehouse='/data/warehouse',
        partition_by=['YEAR', 'MONTH'],
        mode='append',
    )

