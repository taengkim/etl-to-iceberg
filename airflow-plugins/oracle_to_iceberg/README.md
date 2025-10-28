# Oracle to Iceberg Plugin

Oracle 데이터베이스의 데이터를 Apache Iceberg로 이관하는 Airflow Plugin입니다.

## 주요 기능

- ✅ Oracle 테이블 전체 이관
- ✅ CDC 지원: Timestamp, SCN, Flashback
- ✅ Upsert/Merge 지원
- ✅ 청크 단위 처리
- ✅ 자동 타입 변환

## Operator

### 1. OracleToIcebergOperator

Oracle 테이블 데이터를 Iceberg로 일괄 이관합니다.

**주요 파라미터:**
- `oracle_conn_id`: Oracle Airflow Connection ID
- `oracle_schema`: Oracle 스키마명
- `oracle_table`: Oracle 테이블명
- `iceberg_namespace`: Iceberg namespace
- `iceberg_table`: Iceberg 테이블명
- `iceberg_warehouse`: Iceberg warehouse 경로
- `mode`: 쓰기 모드 ('append', 'overwrite')
- `chunksize`: 청크 크기
- `partition_by`: 파티션 컬럼 목록

**사용 예:**

```python
from oracle_to_iceberg import OracleToIcebergOperator

transfer_task = OracleToIcebergOperator(
    task_id='transfer',
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    iceberg_warehouse='/data/warehouse',
    mode='append',
    chunksize=10000,
)
```

### 2. OracleToIcebergCDCOperator

CDC를 통한 증분 동기화를 수행합니다.

**CDC 방법:**
- `timestamp`: 타임스탬프 컬럼 기반
- `scn`: Oracle SCN 기반
- `flashback`: Flashback Query 사용

**사용 예:**

```python
from oracle_to_iceberg import OracleToIcebergCDCOperator

cdc_task = OracleToIcebergCDCOperator(
    task_id='cdc_sync',
    oracle_conn_id='oracle_default',
    oracle_schema='HR',
    oracle_table='EMPLOYEES',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    iceberg_warehouse='/data/warehouse',
    cdc_method='timestamp',
    timestamp_column='LAST_UPDATED',
    mode='upsert',
    primary_key=['EMPLOYEE_ID'],
)
```

## Hook

### OracleHook

Oracle 데이터베이스 연결을 제공합니다.

```python
from oracle_to_iceberg.hooks import OracleHook

hook = OracleHook(oracle_conn_id='oracle_default')
result = hook.execute("SELECT * FROM EMPLOYEES", return_result=True)
```

## 데이터 타입 매핑

| Oracle | Iceberg |
|--------|---------|
| NUMBER | LongType |
| FLOAT | DoubleType |
| VARCHAR2 | StringType |
| DATE, TIMESTAMP | TimestampType |
| BLOB | BinaryType |

## 참고

- [utils/spark_builder.py](../utils/spark_builder.py) - Spark Session 생성
- [example DAGs](../../airflow-dags/oracle_to_iceberg_full_load.py) - 사용 예제

