# Airflow Plugins

Oracle 및 HDFS to Iceberg 데이터 이관을 위한 Custom Airflow Plugins입니다.

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
├── hdfs_to_iceberg/                     # HDFS → Iceberg 이관 플러그인
│   ├── __init__.py
│   ├── hooks.py                         # HDFS 연결 Hook
│   ├── hdfs_to_iceberg_operator.py      # 전체 데이터 이관 Operator
│   └── hdfs_to_iceberg_cdc_operator.py  # CDC 지원 Operator (Partition별 추적)
│
├── maintenance/                         # Iceberg 테이블 유지보수 플러그인
│   ├── __init__.py
│   ├── iceberg_snapshot_operator.py     # 스냅샷 관리 Operator
│   ├── iceberg_compaction_operator.py   # 데이터 압축 Operator
│   └── iceberg_aging_operator.py        # 오래된 데이터 삭제 Operator
│
├── spark/                              # Spark 실행 플러그인
│   ├── __init__.py
│   ├── keycloak_spark_operator.py       # Keycloak 인증 Spark Operator
│   └── example_function_usage.py        # 함수 사용 예제
│
└── utils/                               # 공유 유틸리티 모듈
    ├── __init__.py
    ├── catalog_manager.py               # Iceberg Catalog 관리
    ├── dataframe_utils.py               # DataFrame 처리
    ├── function_converter.py            # 함수 → 스크립트 변환
    ├── keycloak_auth.py                # Keycloak 인증
    ├── minio_manager.py                 # MinIO Storage 관리
    ├── schema_builder.py               # Iceberg 스키마 생성
    ├── spark_builder.py                # Spark 설정 및 Session 생성
    └── type_converter.py               # Oracle → Iceberg 타입 변환
```

## 플러그인 상세 설명

자세한 내용은 각 폴더의 README를 참조하세요:

- 📁 [oracle_to_iceberg/README.md](oracle_to_iceberg/README.md) - Oracle to Iceberg 이관
- 📁 [hdfs_to_iceberg/README.md](hdfs_to_iceberg/README.md) - HDFS to Iceberg 이관
- 📁 [maintenance/README.md](maintenance/README.md) - Iceberg 테이블 유지보수
- 📁 [spark/README.md](spark/README.md) - Spark 작업 실행
- 📁 [utils/README.md](utils/README.md) - 공유 유틸리티

## 주요 기능

### Oracle to Iceberg
- ✅ Oracle 테이블을 Iceberg로 일괄 이관
- ✅ CDC 지원 (Timestamp, SCN, Flashback)
- ✅ Upsert/Merge 지원
- ✅ 청크 단위 처리로 메모리 효율성

### HDFS to Iceberg  
- ✅ HDFS ORC 파일을 Iceberg Parquet로 변환
- ✅ CDC 지원 (파일 변경점 감지)
- ✅ **Partition별 추적**: 이미 지나간 dt(날짜) partition 변경 감지 및 재처리
- ✅ Spark를 통한 대용량 데이터 처리

### Iceberg Maintenance
- ✅ 스냅샷 관리
- ✅ 파일 압축
- ✅ 오래된 데이터 삭제

### Spark Integration
- ✅ Keycloak OAuth2 인증
- ✅ Kubernetes에서 Spark 실행
- ✅ 함수 → 스크립트 변환

## 설치 및 사용

### 1. Airflow에 Plugin 등록

```bash
# 로컬 환경
cp -r airflow-plugins $AIRFLOW_HOME/plugins/

# Kubernetes 환경
# Git Sync를 통해 자동 동기화
```

### 2. DAG에서 사용

```python
from hdfs_to_iceberg import HdfsToIcebergOperator

 عمل = HdfsToIcebergOperator(
    task_id='transfer',
    hdfs_path='hdfs://namenode:9000/data/mytable',
    iceberg_namespace='analytics',
    iceberg_table='mytable',
    catalog_name='iceberg',
    catalog_uri='http://iceberg-rest:8181',
    mode='append',
)
```

## 참고

- [Airflow Plugin Development](https://airflow.apache.org/docs/apache-airflow/stable/howto/create-operator.html)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Spark](https://spark.apache.org/)
