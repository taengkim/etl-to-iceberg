"""
HDFS to Iceberg CDC DAG

HDFS ORC 파일의 변경점을 감지하여 Iceberg로 CDC 이관하는 예제입니다.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from hdfs_to_iceberg import HdfsToIcebergCDCOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'hdfs_to_iceberg_cdc',
    default_args=default_args,
    description='HDFS ORC to Iceberg CDC',
    schedule_interval='@hourly',  # 매시간 변경된 파일 처리
    catchup=False,
    params={  # DAG 레벨 공통 설정
        'catalog_name': 'iceberg',
        'catalog_uri': 'http://iceberg-rest:8181',
        'warehouse_path': 's3://iceberg/warehouse',
    },
    tags=['hdfs', 'iceberg', 'cdc'],
)

# HDFS 파일 변경점 감지 및 CDC 이관
cdc_task = HdfsToIcebergCDCOperator(
    task_id='hdfs_cdc_transfer',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    cdc_method='mtime',  # 수정 시간 기반 변경점 감지
    mode='append',
)

cdc_task

