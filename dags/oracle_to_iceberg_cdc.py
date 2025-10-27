"""
Oracle to Iceberg CDC DAG

변경 데이터 캡처(CDC)를 사용하여 Oracle에서 Iceberg로 증분 동기화하는 DAG입니다.
"""

from airflow import DAG
from datetime import datetime, timedelta
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
    'oracle_to_iceberg_cdc',
    default_args=default_args,
    description='CDC from Oracle to Iceberg',
    schedule='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oracle', 'iceberg', 'cdc'],
) as dag:
    
    cdc_transactions = OracleToIcebergCDCOperator(
        task_id='cdc_transactions',
        oracle_conn_id='oracle_default',
        oracle_schema='SALES',
        oracle_table='TRANSACTIONS',
        iceberg_namespace='analytics',
        iceberg_table='transactions',
        iceberg_warehouse='/data/warehouse',
        cdc_method='timestamp',
        timestamp_column='TRANSACTION_DATE',
        mode='append',
    )

