# Airflow DAGs

Oracle 및 HDFS to Iceberg 데이터 이관을 위한 Airflow DAG 예제 모음입니다.

## DAG 목록

### Oracle Example DAGs

#### 1. oracle_to_iceberg_full_load.py
**전체 데이터 이관 (PyIceberg 사용)**

- PyIceberg 엔진 사용
- Append 모드
- 두 개의 테이블 순차 이관

#### 2. oracle_to_iceberg_pyspark.py
**PySpark를 사용한 대용량 데이터 처리**

- PySpark 엔진 사용
- 50,000 rows per chunk
- Spark 리소스 최적화

#### 3. oracle_to_iceberg_cdc.py
**변경 데이터 캡처 (CDC)**

- Timestamp 기반 증분 동기화
- 매시간 실행

#### 4. oracle_to_iceberg_full_process.py
**초기 로드 후 CDC**

- 초기 전체 로드 (overwrite)
- CDC 동기화 (upsert)
- Primary key 기반 중복 제거

#### 5. oracle_to_iceberg_partitioned.py
**파티션된 테이블 이관**

- YEAR, MONTH로 파티션
- 대용량 데이터 효율적 관리

#### 6. oracle_to_iceberg_realtime_cdc.py
**실시간 CDC (5분마다)**

- 고빈도 동기화
- 실시간 데이터 복제

### HDFS Example DAGs

#### 1. hdfs_to_iceberg_full_load.py
**HDFS ORC 전체 로드**

- HDFS ORC 파일을 Iceberg Parquet로 변환
- 일괄 처리

#### 2. hdfs_to_iceberg_cdc.py
**HDFS CDC 동기화**

- 파일 변경점 감지
- 매시간 실행
- **Partition별 추적 지원**

#### 3. hdfs_to_iceberg_partitioned.py
**Partition 단위 처리**

- 모든 partition 한번에 처리
- 특정 partition만 처리
- 각 partition 개별 처리

### Iceberg Maintenance

#### 1. iceberg_maintenance.py
**Iceberg 유지보수 (압축 + Aging)**

- 파일 압축 (512MB 목표)
- 오래된 스냅샷 삭제 (30일 이상)

#### 2. complete_workflow.py
**전체 워크플로우**

- Oracle 이관 → 압축 → 스냅샷 관리
- 3단계 워크플로우

#### 3. iceberg_snapshot_management.py
**스냅샷 관리**

- 스냅샷 목록 조회
- 관리 작업 수행

## HDFS to Iceberg 특별 기능

### Partition별 추적

`hdfs_to_iceberg_cdc.py` DAG는 이미 지나간 날짜(dt) partition에 대해서도 변경점을 추적합니다:

```python
# 예: dt=2024-01-01이 이미 처리된 후
# 나중에 해당 partition의 파일이 수정되면
# CDC Operator가 자동으로 감지하고 재처리
```

## 필요 설정

### Oracle Connection

```bash
airflow connections add oracle_default \
    --conn-type oracle \
    --conn-host your-oracle-host \
    --conn-port 1521 \
    --conn-schema your_schema \
    --conn-login your_username \
    --conn-password your_password
```

### HDFS Connection

```bash
airflow connections add hdfs_default \
    --conn-type http \
    --conn-host your-namenode \
    --conn-port 9870
```

### Iceberg 설정

DAG params 또는 환경 변수로 설정:

```python
dag = DAG(
    'my_dag',
    params={
        'catalog_name': 'iceberg',
        'catalog_uri': 'http://iceberg-rest:8181',
        'warehouse_path': 's3://iceberg/warehouse',
    },
)
```

## 커스터마이징

각 DAG는 실제 환경에 맞게 수정하여 사용하세요:

1. **Connection ID**: `oracle_conn_id`, `hdfs_conn_id` 변경
2. **스키마/테이블명**: 실제 스키마와 테이블 사용
3. **스케줄**: `schedule` 파라미터 변경
4. **리소스**: 작업 크기에 맞게 Spark 설정 조정
