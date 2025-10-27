"""
Oracle to Iceberg Real-time CDC DAG

실시간으로 변경 데이터를 캡처하여 Iceberg로 동기화하는 DAG입니다.
매 5분마다 실행됩니다.
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
    'oracle_to_iceberg_realtime_cdc',
    default_args=default_args,
    description='Real-time CDC (every 5 minutes)',
    schedule='*/5 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oracle', 'iceberg', 'realtime', 'cdc'],
) as dag:
    
    realtime_cdc = OracleToIcebergCDCOperator(
        task_id='realtime_cdc_orders',
        oracle_conn_id='oracle_default',
        oracle_schema='SALES',
        oracle_table='REAL_TIME_ORDERS',
        iceberg_namespace='realtime',
        iceberg_table='orders',
        iceberg_warehouse='/data/warehouse',
        cdc_method='timestamp',
        timestamp_column='CREATE_TIME',
        mode='append',
    )

