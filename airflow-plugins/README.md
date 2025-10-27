# Airflow Plugins

Oracle to Iceberg 데이터 이관을 위한 Custom Airflow Plugins입니다.

## 폴더 구조

```
airflow-plugins/
├── __init__.py                          # 통합 플러그인 등록
│
├── oracle_to_iceberg/                   # Oracle → Iceberg 이관 플러그인
│   ├── __init__.py
│   ├── hooks.py                         # Oracle 연결 Hook
│   ├── oracle_to_iceberg_operator.py   # 전체 데이터 이관 Operator
│   └── oracle_to_iceberg_cdc_operator.py # CDC 지원 Operator
│
├── maintenance/                         # Iceberg 테이블 유지보수 플러그인
│   ├── __init__.py
│   ├── iceberg_snapshot_operator.py     # 스냅샷 관리 Operator
│   ├── iceberg_compaction_operator.py   # 데이터 압축 Operator
│   └── iceberg_aging_operator.py        # 오래된 데이터 삭제 Operator
│
└── utils/                               # 공유 유틸리티 모듈
    ├── __init__.py
    ├── catalog_manager.py               # Iceberg Catalog 관리
    ├── dataframe_utils.py               # DataFrame 처리
    ├── minio_manager.py                  # MinIO Storage 관리
    ├── schema_builder.py                # Iceberg 스키마 생성
    └── type_converter.py                # Oracle → Iceberg 타입 변환
```

## 플러그인 설명

### 1. Oracle to Iceberg Operator

#### 1.1. OracleToIcebergOperator
**전체 데이터 이관**

**기능:**
- Oracle 테이블 데이터를 Iceberg로 일괄 이관
- PyIceberg 또는 PySpark 엔진 지원
- 청크 단위 처리로 메모리 효율성
- 파티션 지원

**파라미터:**
```python
OracleToIcebergOperator(
    task_id='transfer',
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    iceberg_warehouse='/data/warehouse',
    mode='append',
    write_engine='pyiceberg',
    chunksize=10000,
    partition_by=['department_id'],
)
```

#### 1.2. OracleToIcebergCDCOperator
**변경 데이터 캡처 (CDC)**

**기능:**
- Timestamp, SCN, Flashback 방법 지원
- 증분 동기화
- Upsert/Merge 지원
- 자동 메타데이터 관리

**파라미터:**
```python
OracleToIcebergCDCOperator(
    task_id='cdc_sync',
    oracle_conn_id='oracle_default',
    oracle_schema='SALES',
    oracle_table='ORDERS',
    iceberg_namespace='analytics',
    iceberg_table='orders',
    iceberg_warehouse='/data/warehouse',
    cdc_method='timestamp',
    timestamp_column='LAST_UPDATED',
    mode='upsert',
    primary_key=['ORDER_ID'],
)
```

**CDC 방법:**
- **timestamp**: 타임스탬프 컬럼 기반 변경 추적
- **scn**: Oracle SCN 기반 변경 추적
- **flashback**: Flashback Query 사용

---

### 2. Iceberg Maintenance Operators

#### 2.1. IcebergSnapshotOperator
**스냅샷 관리**

**기능:**
- 스냅샷 생성
- 스냅샷 목록 조회
- 스냅샷 롤백
- 스냅샷 Cherry-pick

**파라미터:**
```python
IcebergSnapshotOperator(
    task_id='manage_snapshots',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    action='list',  # 'create', 'list', 'rollback', 'cherrypick'
    snapshot_id=123,
    minio_endpoint='http://minio:9000',
    minio_access_key='minioadmin',
    minio_secret_key='minioadmin',
    minio_bucket='iceberg',
)
```

#### 2.2. IcebergCompactionOperator
**데이터 압축**

**기능:**
- 작은 파일들을 큰 파일로 병합
- 쿼리 성능 개선
- PySpark 또는 PyIceberg 사용

**파라미터:**
```python
IcebergCompactionOperator(
    task_id='compact',
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    file_size_mb=512,
    use_pyspark=True,
)
```

#### 2.3. IcebergAgingOperator
**오래된 스냅샷 삭제**

**기능:**
- 지정된 일수 이상 된 스냅샷 삭제
- 최근 N개 스냅샷 보존
- 스토리지 정리

**파라미터:**
```python
IcebergAgingOperator(
    task_id='expire_snapshots',
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    older_than_days=30,
    retain_last=10,
)
```

---

### 3. Hooks

#### OracleHook
**Oracle 데이터베이스 연결**

**사용 예:**
```python
from airflow_plugins.oracle_to_iceberg.hooks import OracleHook

hook = OracleHook(oracle_conn_id='oracle_default')
query = "SELECT * FROM EMPLOYEES"
result = hook.execute(query, return_result=True)
```

---

### 4. Utils

#### 4.1. Type Converter
**타입 변환 모듈**

Oracle 데이터 타입을 Iceberg 타입으로 자동 변환:

```python
from airflow_plugins.utils.type_converter import oracle_to_iceberg_type

iceberg_type = oracle_to_iceberg_type('NUMBER')
# Returns: LongType
```

**지원 타입 매핑:**
- NUMBER → LongType
- FLOAT, BINARY_FLOAT → FloatType
- BINARY_DOUBLE → DoubleType
- VARCHAR2, CHAR → StringType
- DATE, TIMESTAMP → TimestampType
- RAW, BLOB → BinaryType

#### 4.2. Schema Builder
**Iceberg 스키마 생성**

```python
from airflow_plugins.utils.schema_builder import create_iceberg_schema

columns = [
    {'name': 'ID', 'type': 'NUMBER', 'nullable': False},
    {'name': 'NAME', 'type': 'VARCHAR2', 'length': 100, 'nullable': True},
]
schema = create_iceberg_schema(columns, primary_keys=['ID'])
```

#### 4.3. Catalog Manager
**Iceberg Catalog 관리**

```python
from airflow_plugins.utils.catalog_manager import (
    create_iceberg_catalog_config,
    load_or_create_catalog,
    ensure_namespace,
)

config = create_iceberg_catalog_config()
catalog = load_or_create_catalog('default', config)
ensure_namespace(catalog, 'analytics')
```

#### 4.4. MinIO Manager
**MinIO Storage 관리**

```python
from airflow_plugins.utils.minio_manager import (
    create_minio_catalog,
    get_iceberg_table,
)

catalog = create_minio_catalog(
    catalog_name='minio_catalog',
    minio_endpoint='http://minio:9000',
    minio_access_key='minioadmin',
    minio_secret_key='minioadmin',
    minio_bucket='iceberg',
    warehouse_path='s3://iceberg/warehouse',
)

table = get_iceberg_table(
    catalog, 
    namespace='analytics', 
    table='employees'
)
```

#### 4.5. DataFrame Utils
**DataFrame 처리**

```python
from airflow_plugins.utils.dataframe_utils import prepare_dataframe

df = prepare_dataframe(df)
# - 컬럼명 소문자로 변환
# - Null 값 처리
# - 날짜 타입 변환
```

---

## 설치 및 사용

### 1. Airflow에 Plugin 등록

Plugin 폴더가 Airflow의 `plugins` 디렉토리에 있어야 합니다:

```bash
# 로컬 환경
cp -r airflow-plugins $AIRFLOW_HOME/plugins/

# Kubernetes 환경 (Git Sync 사용)
# Git 저장소에 포함되어 자동으로 동기화됨
```

### 2. DAG에서 사용

```python
from airflow_plugins.oracle_to_iceberg.oracle_to_iceberg_operator import OracleToIcebergOperator
from airflow_plugins.maintenance.iceberg_compaction_operator import IcebergCompactionOperator

transfer = OracleToIcebergOperator(
    task_id='transfer',
    ...
)
```

---

## 개발 및 확장

### Custom Operator 추가

1. `airflow-plugins/` 폴더에 새 모듈 생성
2. `BaseOperator` 상속
3. `execute()` 메서드 구현
4. `__init__.py`에 등록

### 의존성 추가

```bash
# requirements.txt에 추가
pip install -r requirements.txt
```

---

## 트러블슈팅

### Plugin이 로드되지 않음
- 플러그인 폴더 위치 확인
- Python import 경로 확인
- Airflow 로그 확인

### Operator 실행 실패
- Oracle/Iceberg 연결 확인
- 필수 파라미터 확인
- 로그 파일 확인

---

## 참고

- [Airflow Plugin Development](https://airflow.apache.org/docs/apache-airflow/stable/howto/create-operator.html)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Apache Iceberg](https://iceberg.apache.org/)

