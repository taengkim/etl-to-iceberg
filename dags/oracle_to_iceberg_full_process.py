"""
Oracle to Iceberg Full Process DAG

초기 전체 로드 후 CDC를 통한 증분 동기화를 수행하는 전체 프로세스 DAG입니다.
"""

from airflow import DAG
from datetime import datetime, timedelta
from plugins.oracle_to_iceberg.oracle_to_iceberg_operator import OracleToIcebergOperator
from plugins.oracle_to_iceberg.oracle_to_iceberg_cdc_operator import OracleToIcebergCDCOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'oracle_to_iceberg_full_process',
    default_args=default_args,
    description='Initial load then CDC',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oracle', 'iceberg', 'cdc', 'full-process'],
) as dag:
    
    # Task 1: 초기 전체 로드
    initial_load_products = OracleToIcebergOperator(
        task_id='initial_load_products',
        oracle_conn_id='oracle_default',
        oracle_schema='INVENTORY',
        oracle_table='PRODUCTS',
        iceberg_namespace='analytics',
        iceberg_table='products',
        iceberg_warehouse='/data/warehouse',
        mode='overwrite',
    )
    
    # Task 2: CDC 동기화
    cdc_products = OracleToIcebergCDCOperator(
        task_id='cdc_products',
        oracle_conn_id='oracle_default',
        oracle_schema='INVENTORY',
        oracle_table='PRODUCTS',
        iceberg_namespace='analytics',
        iceberg_table='products',
        iceberg_warehouse='/data/warehouse',
        cdc_method='timestamp',
        timestamp_column='LAST_UPDATED',
        mode='upsert',
        primary_key=['PRODUCT_ID'],
    )
    
    # Task 의존성 설정
    initial_load_products >> cdc_products

