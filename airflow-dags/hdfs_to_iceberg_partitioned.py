"""
HDFS to Iceberg Partitioned DAG

Partition 단위로 HDFS ORC 파일을 Iceberg로 이관하는 예제입니다.
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
    'hdfs_to_iceberg_partitioned',
    default_args=default_args,
    description='HDFS ORC to Iceberg Partitioned Transfer',
    schedule_interval='@daily',
    catchup=False,
    params={  # DAG 레벨 공통 설정
        'catalog_name': 'iceberg',
        'catalog_uri': 'http://iceberg-rest:8181',
        'warehouse_path': 's3://iceberg/warehouse',
    },
    tags=['hdfs', 'iceberg', 'partition'],
)

# 예시 1: 모든 partition을 한번에 처리
transfer_all_partitions = HdfsToIcebergOperator(
    task_id='transfer_all_partitions',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable/dt',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    partition_column='dt',
    process_partitions_separately=False,  # 모든 partition을 한번에 처리
    mode='append',
)

# 예시 2: 특정 partition만 처리
transfer_specific_partitions = HdfsToIcebergOperator(
    task_id='transfer_specific_partitions',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable/dt',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    partition_column='dt',
    partition_value=['2024-01-01', '2024-01-02'],  # 특정 날짜만 처리
    mode='append',
)

# 예시 3: 각 partition을 개별적으로 처리 (리소스 분산 처리)
transfer_separately = HdfsToIcebergOperator(
    task_id='transfer_partitions_separately',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable/dt',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    partition_column='dt',
    process_partitions_separately=True,  # 각 partition을 개별적으로 처리
    mode='append',
    spark_config={
        'spark.executor.memory': '2g',
        'spark.executor.cores': '1',
    },
)

# 워크플로우 설정
transfer_all_partitions >> transfer_specific_partitions >> transfer_separately

