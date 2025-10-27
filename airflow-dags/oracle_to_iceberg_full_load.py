"""
Oracle to Iceberg Full Load DAG

Oracle 테이블을 Iceberg로 전체 이관하는 DAG입니다.
PyIceberg 엔진을 사용합니다.
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
    'oracle_to_iceberg_full_load',
    default_args=default_args,
    description='Full load from Oracle to Iceberg using PyIceberg',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oracle', 'iceberg', 'full-load'],
) as dag:
    
    transfer_employees = OracleToIcebergOperator(
        task_id='transfer_employees',
        oracle_conn_id='oracle_default',
        oracle_schema='HR',
        oracle_table='EMPLOYEES',
        iceberg_namespace='analytics',
        iceberg_table='employees',
        iceberg_warehouse='/data/warehouse',
        mode='append',
        write_engine='pyiceberg',
    )
    
    transfer_departments = OracleToIcebergOperator(
        task_id='transfer_departments',
        oracle_conn_id='oracle_default',
        oracle_schema='HR',
        oracle_table='DEPARTMENTS',
        iceberg_namespace='analytics',
        iceberg_table='departments',
        iceberg_warehouse='/data/warehouse',
        mode='append',
    )
    
    # Task 의존성
    transfer_employees >> transfer_departments

