# Airflow DAGs

Oracle to Iceberg 데이터 이관을 위한 Airflow DAG 예제 모음입니다.

## DAG 목록

### 1. oracle_to_iceberg_full_load.py
**전체 데이터 이관 (PyIceberg 사용)**

- 목적: Oracle 테이블을 Iceberg로 일괄 이관
- 스케줄: 매일
- 특징:
  - 두 개의 테이블 순차 이관 (employees → departments)
  - PyIceberg 엔진 사용
  - Append 모드

**사용 예:**
```python
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
```

---

### 2. oracle_to_iceberg_pyspark.py
**PySpark를 사용한 대용량 데이터 처리**

- 목적: 대용량 데이터를 분산 처리하여 이관
- 스케줄: 매일
- 특징:
  - PySpark 엔진 사용
  - 50,000 rows per chunk
  - Spark 리소스 최적화

**사용 예:**
```python
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
```

---

### 3. oracle_to_iceberg_cdc.py
**변경 데이터 캡처 (CDC)**

- 목적: Timestamp 기반 증분 동기화
- 스케줄: 매시간
- 특징:
  - Timestamp 컬럼 기반 CDC
  - Append 모드로 변경사항 추적

**사용 예:**
```python
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
```

---

### 4. oracle_to_iceberg_full_process.py
**초기 로드 후 CDC (전체 프로세스)**

- 목적: 초기 전체 로드 후 지속적인 증분 동기화
- 스케줄: 매일
- 특징:
  - 초기 전체 로드 (overwrite)
  - CDC 동기화 (upsert)
  - Primary key 기반 중복 제거

**사용 예:**
```python
# Task 1: 초기 전체 로드
initial_load_products = OracleToIcebergOperator(
    task_id='initial_load_products',
    oracle_conn_id='oracle_default',
    oracle_schema='INVENTORY',
    oracle_table='PRODUCTS',
    iceberg_namespace='analytics',
    iceberg_table='products',
    iceberg_warehouse='/data/warehouse',
    mode='overwrite',  # 초기 로드는 overwrite
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
```

---

### 5. oracle_to_iceberg_partitioned.py
**파티션된 테이블 이관**

- 목적: 파티션을 사용하여 대용량 테이블 이관
- 스케줄: 매일
- 특징:
  - YEAR, MONTH로 파티션
  - 대용량 데이터 효율적 관리

**사용 예:**
```python
transfer_sales_history = OracleToIcebergOperator(
    task_id='transfer_sales_history',
    oracle_conn_id='oracle_default',
    oracle_schema='SALES',
    oracle_table='SALES_HISTORY',
    iceberg_namespace='analytics',
    iceberg_table='sales_history',
    iceberg_warehouse='/data/warehouse',
    partition_by=['YEAR', 'MONTH'],
    mode='append',
)
```

---

### 6. oracle_to_iceberg_realtime_cdc.py
**실시간 CDC (5분마다)**

- 목적: 실시간 변경 데이터 추적
- 스케줄: 5분마다 (`*/5 * * * *`)
- 특징:
  - 고빈도 동기화
  - 실시간 데이터 복제

**사용 예:**
```python
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
```

---

### 7. iceberg_maintenance.py
**Iceberg 유지보수 (압축 + Aging)**

- 목적: MinIO Iceberg 테이블 유지보수
- 스케줄: 주 1회
- 특징:
  - 파일 압축 (512MB 목표)
  - 오래된 스냅샷 삭제 (30일 이상)
  - 최근 10개 스냅샷 유지

**사용 예:**
```python
# DAG params에서 MinIO 설정
dag = DAG(
    'iceberg_maintenance',
    params={
        'minio_endpoint': 'http://minio:9000',
        'minio_access_key': 'minioadmin',
        'minio_secret_key': 'minioadmin',
        'minio_bucket': 'iceberg',
        'warehouse_path': 's3://iceberg/warehouse',
    },
)

# 1. 압축 작업
compact = IcebergCompactionOperator(
    task_id='compact',
    airflow_namespace='analytics',
    iceberg_table='transactions',
    file_size_mb=512,
)

# 2. 오래된 스냅샷 삭제
age = IcebergAgingOperator(
    task_id='expire_snapshots',
    airflow_namespace='analytics',
    iceberg_table='transactions',
    older_than_days=30,
    retain_last=10,
)
```

---

### 8. complete_workflow.py
**전체 워크플로우 (이관 → 압축 → Aging)**

- 목적: Oracle 이관부터 유지보수까지 전체 프로세스
- 스케줄: 매일
- 특징:
  - 3단계 워크플로우
  - 데이터 이관 → 압축 → 스냅샷 관리

**사용 예:**
```python
# Task 1: Oracle에서 Iceberg로 이관
transfer_complete = OracleToIcebergOperator(
    task_id='transfer_complete',
    oracle_conn_id='oracle_default',
    oracle_schema='ANALYTICS',
    oracle_table='DAILY_METRICS',
    iceberg_namespace='analytics',
    iceberg_table='daily_metrics',
    iceberg_warehouse='s3://iceberg/warehouse',
    mode='append',
)

# Task 2: 압축
compact_complete = IcebergCompactionOperator(
    task_id='compact_complete',
    iceberg_namespace='analytics',
    iceberg_table='daily_metrics',
    file_size_mb=512,
)

# Task 3: 오래된 스냅샷 삭제
age_complete = IcebergAgingOperator(
    task_id='age_complete',
    iceberg_namespace='analytics',
    iceberg_table='daily_metrics',
    older_than_days=7,
    retain_last=10,
)

# 의존성 설정
transfer_complete >> compact_complete >> age_complete
```

---

### 9. iceberg_snapshot_management.py
**스냅샷 관리**

- 목적: Iceberg 스냅샷 조회 및 관리
- 스케줄: 매일
- 특징:
  - 스냅샷 목록 조회
  - 관리 작업 수행

**사용 예:**
```python
list_snapshots = IcebergSnapshotOperator(
    task_id='list_snapshots',
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    action='list',
)
```

---

## DAG 사용 방법

### 1. Airflow에 DAG 등록

DAG 파일들이 Airflow가 읽을 수 있는 위치에 있어야 합니다:

```bash
# 로컬 환경
cp *.py $AIRFLOW_HOME/dags/

# Kubernetes 환경 (Git Sync 사용)
# helm/airflow/values.yaml에서 gitSync.repo 설정
```

### 2. Oracle Connection 설정

```bash
airflow connections add oracle_default \
    --conn-type oracle \
    --conn-host your-oracle-host \
    --conn-port 1521 \
    --conn-schema your_schema \
    --conn-login your_username \
    --conn-password your_password
```

### 3. DAG 실행

- Airflow UI에서 DAG를 활성화하고 수동 실행
- 또는 스케줄에 따라 자동 실행

## 필요 설정

### Airflow Connection
- `oracle_default`: Oracle 데이터베이스 연결
- 추가 Connection 필요 시 DAG의 `oracle_conn_id` 변경

### 환경 변수
```bash
export ICEBERG_REST_URI=http://your-iceberg-rest:8181
export ICEBERG_WAREHOUSE=/path/to/warehouse
```

### MinIO 설정 (유지보수 DAG)
- DAG `params`에 설정
- 또는 환경 변수로 설정

## 커스터마이징

각 DAG는 실제 환경에 맞게 수정하여 사용하세요:

1. **Connection ID**: `oracle_conn_id` 변경
2. **스키마/테이블명**: 실제 Oracle 스키마와 테이블 사용
3. **스케줄**: `schedule` 파라미터 변경
4. **리소스**: 작업 크기에 맞게 리소스 조정

## 트러블슈팅

### DAG가 보이지 않음
- 파일이 `dags` 폴더에 있는지 확인
- Python 문법 오류 확인
- Airflow Scheduler 로그 확인

### Task가 실패함
- Oracle Connection 설정 확인
- Iceberg 서비스 접근 가능 여부 확인
- Operator 로그 확인

