# HDFS to Iceberg Plugin

HDFS ORC 파일을 Apache Iceberg Parquet 형식으로 이관하는 Airflow Plugin입니다.

## 주요 기능

- ✅ HDFS ORC → Iceberg Parquet 변환
- ✅ CDC 지원: 파일 변경점 감지 및 증분 이관
- ✅ **Partition별 추적**: 이미 처리된 날짜(dt) partition의 변경점도 감지
- ✅ 대용량 데이터 처리: PySpark를 통한 분산 처리
- ✅ 유연한 설정: DAG에서 catalog 및 warehouse 설정 가능

## Operator

### 1. HdfsToIcebergOperator

**전체 데이터 이관 Operator**

**주요 파라미터:**
- `hdfs_path`: HDFS 경로 (파일 또는 디렉토리)
- `iceberg_namespace`: Iceberg namespace
- `iceberg_table`: Iceberg 테이블명
- `catalog_name`: Iceberg catalog 이름
- `catalog_uri`: Iceberg REST Catalog URI
- `warehouse_path`: Iceberg Warehouse 경로
- `mode`: 쓰기 모드 ('append', 'overwrite')
- `partition_column`: Partition 컬럼명 (예: 'dt')
- `partition_value`: 특정 partition 값만 처리
- `process_partitions_separately`: 각 partition을 개별적으로 처리할지 여부
- `spark_config`: 추가 Spark 설정

**사용 예:**

```python
from hdfs_to_iceberg import HdfsToIcebergOperator

# 기본 사용
transfer_task = HdfsToIcebergOperator(
    task_id='transfer',
    hdfs_path='hdfs://namenode:9000/data/mytable',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    catalog_name='iceberg',
    catalog_uri='http://iceberg-rest:8181',
    mode='append',
)

# Partition 단위 처리
transfer_partition = HdfsToIcebergOperator(
    task_id='transfer_partition',
    hdfs_path='hdfs://namenode:9000/data/mytable/dt',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    partition_column='dt',
    partition_value=['2024-01-01'],  # 특정 날짜만
    mode='append',
)

# 각 partition 개별 처리
transfer_separately = HdfsToIcebergOperator(
    task_id='transfer_separately',
    hdfs_path='hdfs://namenode:9000/data/mytable/dt',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    partition_column='dt',
    process_partitions_separately=True,  # 각 partition 개별 처리
    mode='append',
)
```

### 2. HdfsToIcebergCDCOperator

**CDC 지원 Operator - Partition별 추적**

이 Operator는 **이미 지나간 날짜(dt) partition에 대해서도 변경점을 추적**할 수 있습니다.

**CDC 방법:**
- `mtime`: 파일 수정 시간 기반 (기본값)
- `size`: 파일 크기 기반
- `hash`: 파일 해시 기반 (구현 예정)

**주요 파라미터:**
- `cdc_method`: CDC 방법 ('mtime', 'size', 'hash')
- `mode`: 쓰기 모드 ('append', 'upsert')
- `primary_key`: Primary key (upsert 시 필요)

**사용 예:**

```python
from hdfs_to_iceberg import HdfsToIcebergCDCOperator

# CDC 이관
cdc_task = HdfsToIcebergCDCOperator(
    task_id='cdc_sync',
    hdfs_path='hdfs://namenode:9000/data/mytable',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    catalog_name='iceberg',
    catalog_uri='http://iceberg-rest:8181',
    cdc_method='mtime',
    mode='append',
)
```

**Partition별 추적 동작:**

1. 각 partition(dt)별로 마지막 체크 시간 저장
2. partition 내 파일의 수정 시간이 partition별 마지막 체크 시간보다 최신이면 변경된 것으로 감지
3. 변경된 partition만 다시 처리

예시:
- dt=2024-01-01이 이미 처리됨
- 나중에 dt=2024-01-01의 파일이 수정됨
- CDC Operator가 이 변경을 감지하고 다시 처리

## 주요 특징

### Partition별 추적

```python
# 체크포인트 구조 (자동 관리)
{
    'last_check_time': timestamp,
    'processed_files': [...],
    'partitions': {
        '2024-01-01': {
            'files': ['file1.orc', 'file2.orc'],
            'last_check_time': timestamp
        },
        '2024-01-02': {
            'files': ['file3.orc'],
            'last_check_time': timestamp
        }
    }
}
```

각 partition은 독립적으로 변경점을 추적하므로, 이미 지나간 날짜의 데이터가 수정되어도 감지하고 재처리합니다.

### HDFS Hook

HDFS 파일 시스템 접근을 위한 Hook을 제공합니다:

```python
from hdfs_to_iceberg.hooks import HdfsHook

hook = HdfsHook(hdfs_conn_id='hdfs_default')
files = hook.list_files('/path/to/data', recursive=True)
file_info = hook.get_file_info('/path/to/file.orc')
```

## 설정 방법

### DAG 레벨 설정 (권장)

```python
from airflow import DAG

dag = DAG(
    'hdfs_to_iceberg',
    params={
        'catalog_name': 'iceberg',
        'catalog_uri': 'http://iceberg-rest:8181',
        'warehouse_path': 's3://iceberg/warehouse',
    },
)
```

### Operator 레벨 설정

```python
task = HdfsToIcebergOperator(
    task_id='transfer',
    catalog_name='iceberg',
    catalog_uri='http://iceberg-rest:8181',
    warehouse_path='s3://iceberg/warehouse',
    ...
)
```

## 참고

- [utils/spark_builder.py](../utils/spark_builder.py) - Spark Session 생성
- [utils/function_converter.py](../utils/function_converter.py) - 함수 변환
- [example DAGs](../../airflow-dags/hdfs_to_iceberg_full_load.py) - 사용 예제

