# Iceberg Maintenance Plugin

Apache Iceberg 테이블 유지보수를 위한 Airflow Plugin입니다.

## 주요 기능

- ✅ 스냅샷 관리
- ✅ 파일 압축 (Compaction)
- ✅ 오래된 스냅샷 삭제 (Aging)

## Operator

### 1. IcebergSnapshotOperator

Iceberg 스냅샷을 생성, 조회, 롤백합니다.

**작업 타입:**
- `create`: 스냅샷 생성
- `list`: 스냅샷 목록 조회
- `rollback`: 특정 스냅샷으로 롤백
- `cherrypick`: 특정 스냅샷 Cherry-pick

**사용 예:**

```python
from maintenance import IcebergSnapshotOperator

list_task = IcebergSnapshotOperator(
    task_id='list_snapshots',
    iceberg_namespace='analytics',
    iceberg_table='employees',
    action='list',
)
```

### 2. IcebergCompactionOperator

작은 파일들을 큰 파일로 병합하여 쿼리 성능을 개선합니다.

**주요 파라미터:**
- `file_size_mb`: 목표 파일 크기 (MB, 기본값: 512)
- `use_pyspark`: PySpark 사용 여부 (기본값: True)

**사용 예:**

```python
from maintenance import IcebergCompactionOperator

compact_task = IcebergCompactionOperator(
    task_id='compact',
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    file_size_mb=512,
    use_pyspark=True,
)
```

### 3. IcebergAgingOperator

오래된 스냅샷을 삭제하여 스토리지를 정리합니다.

**주요 파라미터:**
- `older_than_days`: 삭제할 스냅샷 기준 일수 (기본값: 7)
- `retain_last`: 유지할 최근 스냅샷 개수 (기본값: 10)

**사용 예:**

```python
from maintenance import IcebergAgingOperator

aging_task = IcebergAgingOperator(
    task_id='expire_snapshots',
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    older_than_days=30,
    retain_last=10,
)
```

## 전체 유지보수 워크플로우

```python
from airflow import DAG
from maintenance import IcebergCompactionOperator, IcebergAgingOperator

dag = DAG(
    'iceberg_maintenance',
    params={  # DAG 레벨 설정
        'catalog_name': 'iceberg',
        'catalog_uri': 'http://iceberg-rest:8181',
        'warehouse_path': 's3://iceberg/warehouse',
    },
)

# 압축
compact = IcebergCompactionOperator(
    task_id='compact',
    dag=dag,
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    file_size_mb=512,
)

# 스냅샷 정리
age = IcebergAgingOperator(
    task_id='expire_snapshots',
    dag=dag,
    iceberg_namespace='analytics',
    iceberg_table='transactions',
    older_than_days=30,
    retain_last=10,
)

compact >> age
```

## 참고

- [example DAGs](../../airflow-dags/iceberg_maintenance.py) - 사용 예제

