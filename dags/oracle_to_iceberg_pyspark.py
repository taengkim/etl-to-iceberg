"""
Oracle to Iceberg using PySpark DAG

PySpark를 사용하여 대용량 데이터를 Iceberg로 이관하는 DAG입니다.
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
    'oracle_to_iceberg_pyspark',
    default_args=default_args,
    description='Oracle to Iceberg using PySpark',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['oracle', 'iceberg', 'pyspark'],
) as dag:
    
    transfer_orders_pyspark = OracleToIcebergOperator(
        task_id='transfer_orders_pyspark',
        oracle_conn_id='oracle_default',
        oracle_schema='SALES',
        oracle_table='ORDERS',
        iceberg_namespace='analytics',
        iceberg_table='orders',
        iceberg_warehouse='/data/warehouse',
        chunksize=50000,
        mode='append',
        write_engine='pyspark',
        spark_config={
            'spark.executor.memory': '4g',
            'spark.executor.cores': '4',
            'spark.driver.memory': '2g',
        },
    )

