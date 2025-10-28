"""
HDFS to Iceberg Full Load DAG

HDFS ORC 파일을 Iceberg Parquet 테이블로 이관하는 전체 로드 예제입니다.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from hdfs_to_iceberg import HdfsToIcebergOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'hdfs_to_iceberg_full_load',
    default_args=default_args,
    description='HDFS ORC to Iceberg Parquet Full Load',
    schedule_interval='@daily',
    catchup=False,
    params={  # DAG 레벨 공통 설정
        'catalog_name': 'iceberg',
        'catalog_uri': 'http://iceberg-rest:8181',
        'warehouse_path': 's3://iceberg/warehouse',
    },
    tags=['hdfs', 'iceberg', 'etl'],
)

# HDFS ORC 파일을 Iceberg로 이관
transfer_task = HdfsToIcebergOperator(
    task_id='transfer_hdfs_to_iceberg',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    mode='append',
)

transfer_task

