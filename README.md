# ETL to Iceberg Airflow Plugin

Oracle 데이터베이스 및 HDFS의 데이터를 Apache Iceberg 형식으로 이관하는 Airflow 플러그인입니다.

## 주요 기능

- ✅ **전체 데이터 이관**: Oracle 테이블을 Iceberg로 일괄 이관
- ✅ **CDC 지원**: 변경 데이터 캡처를 통한 증분 동기화
- ✅ **다양한 CDC 방법**: Timestamp, SCN, Flashback 지원
- ✅ **Upsert/Merge 지원**: 중복 데이터 처리
- ✅ **대용량 처리**: 청크 단위 처리로 메모리 효율성 확보
- ✅ **자동 타입 변환**: Oracle → Iceberg 타입 자동 매핑
- ✅ **다중 쓰기 엔진**: PyIceberg 또는 PySpark 선택 가능
- ✅ **Iceberg 유지보수**: 스냅샷, 압축, 오래된 데이터 삭제 지원

## 프로젝트 구조

```
airflow-plugins/                   # Airflow Custom Plugins
├── __init__.py                    # 통합 플러그인 등록
│
├── oracle_to_iceberg/             # Oracle → Iceberg 데이터 이관 플러그인
│   ├── __init__.py
│   ├── hooks.py                   # Oracle 연결 Hook
│   ├── oracle_to_iceberg_operator.py        # 전체 데이터 이관 Operator
│   └── oracle_to_iceberg_cdc_operator.py     # CDC 지원 Operator
│
├── hdfs_to_iceberg/               # HDFS → Iceberg 데이터 이관 플러그인
│   ├── __init__.py
│   ├── hooks.py                   # HDFS 연결 Hook
│   ├── hdfs_to_iceberg_operator.py           # 전체 데이터 이관 Operator
│   └── hdfs_to_iceberg_cdc_operator.py       # CDC 지원 Operator
│
├── maintenance/                   # Iceberg 테이블 유지보수 플러그인
│   ├── __init__.py
│   ├── iceberg_snapshot_operator.py         # 스냅샷 관리 Operator
│   ├── iceberg_compaction_operator.py       # 데이터 압축 Operator
│   └── iceberg_aging_operator.py             # 오래된 데이터 삭제 Operator
│
└── utils/                         # 공유 유틸리티 모듈
    ├── __init__.py
    ├── catalog_manager.py         # Iceberg Catalog 관리
    ├── dataframe_utils.py         # DataFrame 처리
    ├── function_converter.py      # 함수 → 스크립트 변환
    ├── keycloak_auth.py          # Keycloak 인증
    ├── minio_manager.py          # MinIO Storage 관리
    ├── schema_builder.py         # Iceberg 스키마 생성
    ├── spark_builder.py          # Spark 설정 및 Session 생성
    └── type_converter.py         # Oracle → Iceberg 타입 변환

airflow-dags/                      # Airflow DAG 예제
├── Oracle Example DAGs/
│   ├── oracle_to_iceberg_full_load.py           # 전체 로드
│   ├── oracle_to_iceberg_pyspark.py            # PySpark 사용
│   ├── oracle_to_iceberg_cdc.py                 # CDC 동기화
│   ├── oracle_to_iceberg_full_process.py       # 초기 로드 후 CDC
│   ├── oracle_to_iceberg_partitioned.py        # 파티션 테이블
│   └── oracle_to_iceberg_realtime_cdc.py       # 실시간 CDC
├── HDFS Example DAGs/
│   ├── hdfs_to_iceberg_full_load.py            # 전체 로드
│   ├── hdfs_to_iceberg_cdc.py                  # CDC 동기화
│   └── hdfs_to_iceberg_partitioned.py          # 파티션 단위 처리
└── Iceberg Maintenance/
    ├── iceberg_maintenance.py                   # 유지보수
    ├── complete_workflow.py                     # 전체 워크플로우
    └── iceberg_snapshot_management.py           # 스냅샷 관리

helm/                              # Kubernetes 배포 (자세한 내용은 helm/README.md 참조)
├── argoapps/                      # ArgoCD Application 매니페스트
│   ├── postgresql-app.yaml
│   ├── redis-app.yaml
│   ├── airflow-app.yaml
│   └── kustomization.yaml
├── postgresql/                    # PostgreSQL Helm values
│   └── values.yaml
├── redis/                         # Redis Helm values
│   └── values.yaml
└── airflow/                       # Airflow Helm values
    └── values.yaml
```

## Kubernetes 배포 (ArgoCD)

이 프로젝트는 Kubernetes 클러스터에 ArgoCD를 통해 배포할 수 있는 Helm 차트를 포함하고 있습니다.

### 구성 요소

- **PostgreSQL**: Airflow 메타데이터 데이터베이스 (Bitnami PostgreSQL)
- **Redis**: Celery 메시지 브로커 (Bitnami Redis)
- **Airflow**: Apache Airflow with Celery Executor

### 빠른 시작

```bash
# ArgoCD Application 배포
kubectl apply -k helm/argoapps/
```

자세한 내용은 [helm/README.md](helm/README.md)를 참조하세요.

## 설치

### 1. 패키지 설치

```bash
pip install -r requirements.txt
```

**참고**: PySpark를 사용하려면 추가로 Iceberg Spark connector가 필요합니다:

```bash
# PySpark with Iceberg
pip install pyspark>=3.4.0

# Spark Iceberg connector는 Spark jars 경로에 추가해야 합니다
# 예: $SPARK_HOME/jars/
```

### 2. Oracle Instant Client 설치

Oracle 데이터베이스 연결을 위해서는 Oracle Instant Client가 필요합니다.

- [Oracle Instant Client 다운로드](https://www.oracle.com/database/technologies/instant-client/downloads.html)
- Windows의 경우 PATH에 추가하거나 시스템 디렉토리에 복사

### 3. Airflow Connection 설정

Airflow UI 또는 CLI를 통해 Oracle 연결을 설정합니다:

```bash
airflow connections add oracle_default \
    --conn-type oracle \
    --conn-host your-oracle-host \
    --conn-port 1521 \
    --conn-schema your_schema \
    --conn-login your_username \
    --conn-password your_password
```

### 4. Iceberg 설정

환경 변수를 설정합니다:

```bash
export ICEBERG_REST_URI=http://your-iceberg-rest:8181
export ICEBERG_WAREHOUSE=/path/to/warehouse
export ICEBERG_AUTH_TYPE=basic  # optional
export ICEBERG_CREDENTIAL=username:password  # optional
```

## 사용 방법

### 1. 전체 데이터 이관 (Full Load)

#### 1.1 PyIceberg 사용 (기본)

```python
from plugins.oracle_to_iceberg.oracle_to_iceberg_operator import OracleToIcebergOperator

transfer_task = OracleToIcebergOperator(
    task_id='transfer_oracle_to_iceberg',
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    iceberg_warehouse='/data/warehouse',
    mode='append',
    write_engine='pyiceberg',  # 기본값
)
```

#### 1.2 PySpark 사용

```python
transfer_task = OracleToIcebergOperator(
    task_id='transfer_with_pyspark',
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    iceberg_warehouse='/data/warehouse',
    mode='append',
    write_engine='pyspark',  # PySpark 사용
    spark_config={
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
    },
)
```

### 2. 쓰기 엔진 비교

| 엔진 | 장점 | 단점 | 사용 시나리오 |
|------|------|------|-------------|
| **PyIceberg** | 가볍고 빠름, 의존성 적음 | 대규모 병렬 처리 제한 | 중소규모 데이터, 단일 노드 |
| **PySpark** | 대규모 병렬 처리, 분산 처리 | 의존성 많음, 리소스 소모 큼 | 대규모 데이터, 클러스터 환경 |

### 3. CDC를 통한 증분 동기화

#### 2.1 Timestamp 기반 CDC

```python
from plugins.oracle_to_iceberg.oracle_to_iceberg_cdc_operator import OracleToIcebergCDCOperator

cdc_task = OracleToIcebergCDCOperator(
    task_id='cdc_transfer',
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='ORDERS',
    iceberg_namespace='analytics',
    iceberg_table='orders',
    iceberg_warehouse='/data/warehouse',
    mode='upsert',
    cdc_method='timestamp',
    timestamp_column='LAST_UPDATED',
    primary_key=['ORDER_ID'],
)
```

#### 2.2 SCN 기반 CDC

```python
cdc_scn_task = OracleToIcebergCDCOperator(
    task_id='cdc_scn_transfer',
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    cdc_method='scn',  # Oracle System Change Number
    mode='upsert',
    primary_key=['EMPLOYEE_ID'],
)
```

#### 2.3 스케줄링된 CDC (매시간)

```python
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

dag = DAG(
    'oracle_cdc_sync',
    schedule_interval='@hourly',  # 매시간 실행
    start_date=days_ago(1),
    catchup=False,
)

cdc_hourly = OracleToIcebergCDCOperator(
    task_id='hourly_cdc',
    dag=dag,
    oracle_conn_id='oracle_default',
    oracle_schema='SALES',
    oracle_table='TRANSACTIONS',
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    iceberg_warehouse='/data/warehouse',
    cdc_method='timestamp',
    timestamp_column='TRANSACTION_DATE',
    mode='append',  # 또는 'upsert' 사용
    primary_key=['TRANSACTION_ID'],
)
```

### 고급 사용 예제

```python
# WHERE 절과 컬럼 선택
transfer_task = OracleToIcebergOperator(
    task_id='transfer_with_filter',
    dag=dag,
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees_filtered',
    columns=['EMPLOYEE_ID', 'FIRST_NAME', 'LAST_NAME', 'SALARY'],
    where_clause="SALARY > 50000 AND DEPARTMENT_ID = 10",
    mode='append',
)

# 청크 단위로 대용량 데이터 처리
transfer_large_table = OracleToIcebergOperator(
    task_id='transfer_large_table',
    dag=dag,
    oracle_conn_id='oracle_default',
    oracle_schema='SALES',
    oracle_table='ORDERS',
    iceberg_namespace='analytics',
    iceberg_table='orders',
    chunksize=10000,  # 10,000 rows per chunk
    mode='append',
)

# 파티션 설정
transfer_partitioned = OracleToIcebergOperator(
    task_id='transfer_partitioned',
    dag=dag,
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='SALARY_HISTORY',
    iceberg_namespace='analytics',
    iceberg_table='salary_history',
    partition_by=['department_id', 'year'],  # 파티션 컬럼
    mode='append',
)

# 실행 날짜 기반 동적 테이블명
transfer_dynamic = OracleToIcebergOperator(
    task_id='transfer_dynamic',
    dag=dag,
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees_{{ ds_nodash }}',  # 20240101 형식
    mode='overwrite',
)
```

## Operator 파라미터

| 파라미터 | 타입 | 필수 | 설명 |
|---------|------|------|------|
| `oracle_conn_id` | str | Yes | Oracle Airflow Connection ID |
| `oracle_schema` | str | Yes | Oracle 스키마명 |
| `oracle_table` | str | Yes | Oracle 테이블명 |
| `iceberg_catalog` | str | No | Iceberg catalog 이름 (기본값: 'default') |
| `iceberg_namespace` | str | Yes | Iceberg namespace (database) |
| `iceberg_table` | str | No | Iceberg 테이블명 (기본값: oracle_table과 동일) |
| `iceberg_warehouse` | str | No | Iceberg warehouse 경로 (환경변수 사용 가능) |
| `columns` | list | No | 추출할 컬럼 목록 (None이면 전체) |
| `where_clause` | str | No | WHERE 절 조건 |
| `chunksize` | int | No | 청크 크기 (대용량 데이터 처리) |
| `mode` | str | No | 쓰기 모드 ('append' 또는 'overwrite') |
| `partition_by` | list | No | 파티션 컬럼 목록 |

## HDFS to Iceberg 사용 방법

### 1. 전체 데이터 이관 (Full Load)

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from hdfs_to_iceberg import HdfsToIcebergOperator

dag = DAG(
    'hdfs_to_iceberg_full_load',
    default_args={
        'owner': 'data-engineering',
        'start_date': days_ago(1),
    },
    schedule_interval='@daily',
    catchup=False,
    params={  # DAG 레벨 설정
        'catalog_name': 'iceberg',
        'catalog_uri': 'http://iceberg-rest:8181',
        'warehouse_path': 's3://iceberg/warehouse',
    },
)

transfer_task = HdfsToIcebergOperator(
    task_id='transfer_hdfs_orc',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    mode='append',
)
```

### 2. Partition 단위 처리

```python
# 특정 partition만 처리
transfer_partition = HdfsToIcebergOperator(
    task_id='transfer_partition',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable/dt',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    partition_column='dt',  # Hive-style partition 컬럼
    partition_value=['2024-01-01', '2024-01-02'],  # 특정 날짜만
    mode='append',
)

# 각 partition을 개별적으로 처리
transfer_separately = HdfsToIcebergOperator(
    task_id='transfer_separately',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable/dt',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    partition_column='dt',
    process_partitions_separately=True,  # 각 partition 개별 처리
    mode='append',
)
```

### 3. CDC를 통한 증분 동기화

```python
from hdfs_to_iceberg import HdfsToIcebergCDCOperator

cdc_task = HdfsToIcebergCDCOperator(
    task_id='hdfs_cdc',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/mytable',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    cdc_method='mtime',  # 수정 시간 기반 변경점 감지
    mode='append',
)
```

### 4. Spark 설정 최적화

```python
transfer_optimized = HdfsToIcebergOperator(
    task_id='transfer_optimized',
    dag=dag,
    hdfs_path='hdfs://namenode:9000/data/large_table',
    iceberg_namespace='analytics',
    iceberg_table='large_table',
    spark_config={
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '4',
    },
    mode='append',
)
```

## 데이터 타입 매핑

Oracle 데이터 타입이 다음 Iceberg 타입으로 자동 변환됩니다:

| Oracle | Iceberg |
|--------|---------|
| NUMBER | LongType |
| FLOAT | DoubleType |
| BINARY_FLOAT | FloatType |
| BINARY_DOUBLE | DoubleType |
| VARCHAR2, CHAR, NCHAR, NVARCHAR2 | StringType |
| CLOB, NCLOB | StringType |
| DATE, TIMESTAMP | TimestampType |
| TIMESTAMP WITH TIME ZONE | TimestamptzType |
| RAW, BLOB | BinaryType |

## 주의사항

1. **대용량 데이터**: 대용량 테이블의 경우 `chunksize` 파라미터를 사용하여 메모리 효율적으로 처리하세요.

2. **Oracle Instant Client**: Oracle 데이터베이스 연결을 위해서는 Oracle Instant Client가 설치되어 있어야 합니다.

3. **Iceberg Catalog**: Iceberg REST catalog 서버가 실행 중이어야 합니다.

4. **권한**: Oracle 및 Iceberg에 대한 적절한 읽기/쓰기 권한이 필요합니다.

## 문제 해결

### "Oracle client library not found" 에러

Oracle Instant Client를 설치하고 PATH에 추가하세요.

### "Iceberg connection failed" 에러

- `ICEBERG_REST_URI` 환경 변수가 올바르게 설정되었는지 확인
- Iceberg REST 서버가 실행 중인지 확인
- 네트워크 접근성 확인

### 메모리 부족 에러

`chunksize` 파라미터를 사용하여 데이터를 청크 단위로 처리하세요.

## 라이선스

이 플러그인은 Apache 2.0 라이선스 하에 배포됩니다.

## CDC (Change Data Capture) 상세 설명

### CDC 방법 비교

| 방법 | 설명 | 장점 | 단점 | 사용 시나리오 |
|------|------|------|------|-------------|
| **timestamp** | 타임스탬프 컬럼으로 변경 추적 | 구현 간단, 이해 쉬움 | 타임스탬프 컬럼 필요 | 일반적인 업데이트 추적 |
| **scn** | Oracle SCN 사용 | 정확한 변경 추적 | Oracle 전용 | Oracle 전용 환경 |
| **flashback** | Flashback Query 사용 | 과거 데이터 복구 가능 | 성능 영향 가능 | 과거 데이터 조회 필요시 |

### CDC 메타데이터 관리

CDC Operator는 자동으로 메타데이터를 관리합니다:

- **자동 추적**: 마지막 처리 시점을 자동으로 저장
- **메타데이터 테이블**: `__airflow_cdc_metadata__` 테이블에 저장
- **재시작 안전**: Airflow 재시작 후에도 정확한 위치에서 재개

### CDC 파라미터

| 파라미터 | 타입 | 필수 | 설명 |
|---------|------|------|------|
| `cdc_method` | str | No | CDC 방법 ('timestamp', 'scn', 'flashback') |
| `timestamp_column` | str | 조건부 | Timestamp 컬럼명 (timestamp 방법 시 필수) |
| `last_timestamp` | str | No | 마지막 처리 타임스탬프 |
| `last_scn` | int | No | 마지막 SCN |
| `primary_key` | list | 조건부 | Primary key (upsert 시 필수) |
| `mode` | str | No | 쓰기 모드 ('append', 'upsert', 'merge') |
| `metadata_table` | str | No | 메타데이터 테이블명 (기본값: __airflow_cdc_metadata__) |

## 사용 예제

### 예제 1: 초기 전체 로드 후 CDC

```python
from airflow import DAG
from datetime import datetime
from plugins.oracle_to_iceberg.oracle_to_iceberg_operator import OracleToIcebergOperator
from plugins.oracle_to_iceberg.oracle_to_iceberg_cdc_operator import OracleToIcebergCDCOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'oracle_full_load_then_cdc',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: 초기 전체 로드 (한 번만 실행)
initial_load = OracleToIcebergOperator(
    task_id='initial_load',
    dag=dag,
    oracle_conn_id='oracle_default',
    oracle_schema='SALES',
    oracle_table='ORDERS',
    iceberg_namespace='analytics',
    iceberg_table='orders',
    mode='overwrite',  # 초기 로드는 overwrite
)

# Task 2: CDC (매일 실행)
cdc_sync = OracleToIcebergCDCOperator(
    task_id='daily_cdc',
    dag=dag,
    oracle_conn_id='oracle_default',
    oracle_schema='SALES',
    oracle_table='ORDERS',
    iceberg_namespace='analytics',
    iceberg_table='orders',
    cdc_method='timestamp',
    timestamp_column='LAST_UPDATED',
    mode='upsert',
    primary_key=['ORDER_ID'],
)

# 의존성 설정
initial_load >> cdc_sync
```

### 예제 2: 실시간 CDC (5분마다)

```python
dag = DAG(
    'realtime_cdc',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # 5분마다
    catchup=False,
)

realtime_cdc = OracleToIcebergCDCOperator(
    task_id='realtime_cdc',
    dag=dag,
    oracle_conn_id='oracle_default',
    oracle_schema='TRANSACTIONS',
    oracle_table='REAL_TIME_TRANS',
    iceberg_namespace='realtime',
    iceberg_table='transactions',
    cdc_method='timestamp',
    timestamp_column='CREATE_TIME',
    mode='append',
)
```

### 예제 3: Upsert vs Append

```python
# Upsert: 중복 제거하여 업데이트
upsert_task = OracleToIcebergCDCOperator(
    task_id='upsert_example',
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    mode='upsert',
    primary_key=['EMPLOYEE_ID'],  # Upsert는 primary_key 필요
    cdc_method='timestamp',
    timestamp_column='LAST_UPDATED',
)

# Append: 모든 변경사항 추가 (중복 허용)
append_task = OracleToIcebergCDCOperator(
    task_id='append_example',
    oracle_conn_id='oracle_default',
    oracle_schema='SALES',
    oracle_table='SALES_LOG',
    iceberg_namespace='analytics',
    iceberg_table='sales_log',
    mode='append',  # Append는 primary_key 불필요
    cdc_method='timestamp',
    timestamp_column='SALE_DATE',
)
```

## CDC 주의사항

1. **타임스탬프 컬럼**: timestamp 방법 사용 시 테이블에 타임스탬프 컬럼이 필요합니다.
2. **Oracle 권한**: SCN 방법 사용 시 `V$DATABASE` 조회 권한 필요
3. **Flashback**: Flashback Query 사용 시 충분한 UNDO 공간 필요
4. **Primary Key**: Upsert 사용 시 반드시 primary_key를 지정하세요

## PySpark 사용 시 주의사항

1. **Spark 설치**: PySpark와 Iceberg Spark connector가 필요합니다.
2. **Spark Session**: Operator가 자동으로 SparkSession을 생성합니다.
3. **리소스**: Spark executor 메모리와 CPU 설정을 적절히 조정하세요.
4. **클러스터**: 분산 클러스터 사용 시 YARN이나 Kubernetes 설정 필요

## Iceberg 유지보수 Operator

MinIO에 저장된 Iceberg 테이블을 관리하는 Operator들을 제공합니다.

### 1. 스냅샷 관리 (Snapshot)

```python
from plugins.maintenance.iceberg_snapshot_operator import IcebergSnapshotOperator

# 현재 스냅샷 정보 조회
snapshot_task = IcebergSnapshotOperator(
    task_id='list_snapshots',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    minio_endpoint='http://minio:9000',
    minio_access_key='minioadmin',
    minio_secret_key='minioadmin',
    minio_bucket='iceberg',
    action='list',
)
```

### 2. 데이터 압축 (Compaction)

```python
from plugins.maintenance.iceberg_compaction_operator import IcebergCompactionOperator

# 작은 파일들을 큰 파일로 병합
compaction_task = IcebergCompactionOperator(
    task_id='compact_table',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    minio_endpoint='http://minio:9000',
    minio_access_key='minioadmin',
    minio_secret_key='minioadmin',
    minio_bucket='iceberg',
    file_size_mb=512,  # 목표 파일 크기 (512MB)
    use_pyspark=True,  # PySpark 사용 (기본값)
)
```

### 3. 오래된 스냅샷 삭제 (Aging)

```python
from plugins.maintenance.iceberg_aging_operator import IcebergAgingOperator

# 7일 이상 된 스냅샷 삭제 (최근 10개는 유지)
aging_task = IcebergAgingOperator(
    task_id='expire_old_snapshots',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    minio_endpoint='http://minio:9000',
    minio_access_key='minioadmin',
    minio_secret_key='minioadmin',
    minio_bucket='iceberg',
    older_than_days=7,  # 7일 이전 스냅샷 삭제
    retain_last=10,      # 최근 10개 스냅샷 유지
    use_pyspark=True,
)
```

### 4. 전체 유지보수 워크플로우

```python
from airflow import DAG
from datetime import datetime
from plugins.maintenance.iceberg_compaction_operator import IcebergCompactionOperator
from plugins.maintenance.iceberg_aging_operator import IcebergAgingOperator

dag = DAG(
    'iceberg_maintenance',
    default_args={
        'owner': 'data-engineering',
        'start_date': datetime(2024, 1, 1),
    },
    schedule_interval='@weekly',  # 주 1회 실행
    catchup=False,
    params={  # DAG 레벨에서 MinIO 설정 지정
        'minio_endpoint': 'http://minio:9000',
        'minio_access_key': 'minioadmin',
        'minio_secret_key': 'minioadmin',
        'minio_bucket': 'iceberg',
        'warehouse_path': 's3://iceberg/warehouse',
    },
)

# 1. 압축 (MinIO 설정은 DAG params에서 자동으로 가져옴)
compact = IcebergCompactionOperator(
    task_id='compact',
    dag=dag,
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    file_size_mb=512,
)

# 2. 오래된 스냅샷 삭제 (MinIO 설정은 DAG params에서 자동으로 가져옴)
age = IcebergAgingOperator(
    task_id='expire_snapshots',
    dag=dag,
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    older_than_days=30,
    retain_last=10,
)

# 의존성 설정
compact >> age
```

### Iceberg 유지보수 Operator 파라미터

#### IcebergSnapshotOperator

| 파라미터 | 타입 | 필수 | 설명 |
|---------|------|------|------|
| `iceberg_namespace` | str | Yes | Iceberg namespace |
| `iceberg_table` | str | Yes | Iceberg 테이블명 |
| `minio_endpoint` | str | No | MinIO endpoint (환경변수 사용 가능) |
| `minio_access_key` | str | No | MinIO Access Key |
| `minio_secret_key` | str | No | MinIO Secret Key |
| `minio_bucket` | str | No | MinIO Bucket 이름 |
| `action` | str | No | 작업 ('create', 'list', 'rollback', 'cherrypick') |
| `snapshot_id` | int | No | 스냅샷 ID (rollback/cherrypick 시 사용) |
| `warehouse_path` | str | No | Warehouse 경로 |

#### IcebergCompactionOperator

| 파라미터 | 타입 | 필수 | 설명 |
|---------|------|------|------|
| `iceberg_namespace` | str | Yes | Iceberg namespace |
| `iceberg_table` | str | Yes | Iceberg 테이블명 |
| `minio_endpoint` | str | No | MinIO endpoint |
| `minio_access_key` | str | No | MinIO Access Key |
| `minio_secret_key` | str | No | MinIO Secret Key |
| `minio_bucket` | str | No | MinIO Bucket 이름 |
| `file_size_mb` | int | No | 목표 파일 크기 (MB, 기본값: 512) |
| `use_pyspark` | bool | No | PySpark 사용 여부 (기본값: True) |
| `warehouse_path` | str | No | Warehouse 경로 |

#### IcebergAgingOperator

| 파라미터 | 타입 | 필수 | 설명 |
|---------|------|------|------|
| `iceberg_namespace` | str | Yes | Iceberg namespace |
| `iceberg_table` | str | Yes | Iceberg 테이블명 |
| `minio_endpoint` | str | No | MinIO endpoint |
| `minio_access_key` | str | No | MinIO Access Key |
| `minio_secret_key` | str | No | MinIO Secret Key |
| `minio_bucket` | str | No | MinIO Bucket 이름 |
| `older_than_days` | int | No | 삭제할 스냅샷 기준 일수 (기본값: 7) |
| `retain_last` | int | No | 유지할 최근 스냅샷 개수 (기본값: 10) |
| `use_pyspark` | bool | No | PySpark 사용 여부 (기본값: True) |
| `warehouse_path` | str | No | Warehouse 경로 |

## MinIO 설정 방법

MinIO 설정은 다음 세 가지 방법으로 지정할 수 있습니다 (우선순위 높은 순):

### 1. DAG params (권장)

DAG 레벨에서 params로 설정하면 모든 Task가 공유합니다:

```python
from airflow import DAG
from plugins.maintenance.iceberg_compaction_operator import IcebergCompactionOperator

dag = DAG(
    'iceberg_maintenance',
    params={  # DAG 레벨 params
        'minio_endpoint': 'http://minio:9000',
        'minio_access_key': 'minioadmin',
        'minio_secret_key': 'minioadmin',
        'minio_bucket': 'iceberg',
        'warehouse_path': 's3://iceberg/warehouse',
    },
)

# MinIO 설정을 각 Operator에 명시하지 않아도 DAG params에서 자동으로 가져옵니다
compact = IcebergCompactionOperator(
    task_id='compact',
    dag=dag,
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    file_size_mb=512,
)
```

### 2. Operator 파라미터

각 Operator에 직접 설정:

```python
compact = IcebergCompactionOperator(
    task_id='compact',
    dag=dag,
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    minio_endpoint='http://minio:9000',
    minio_access_key='minioadmin',
    minio_secret_key='minioadmin',
    minio_bucket='iceberg',
    file_size_mb=512,
)
```

### 3. 환경 변수

전역 환경 변수 설정:

```bash
# MinIO 설정 (가장 낮은 우선순위)
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET=iceberg
```
