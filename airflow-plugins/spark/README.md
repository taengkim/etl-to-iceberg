# Spark Plugin for Airflow

Kubernetes에서 PySpark 작업을 실행하기 위한 Airflow Plugin입니다.

## 개요

이 Plugin은 Kubernetes 클러스터에서 Spark 작업을 실행하고, Keycloak을 통한 OAuth2 인증을 지원합니다.

## 주요 기능

- ✅ **Kubernetes Native**: Kubernetes에서 Spark Driver를 Pod로 실행
- ✅ **Keycloak 인증**: OAuth2 액세스 토큰을 통한 보안 인증
- ✅ **유연한 스크립트**: Python 스크립트 직접 실행 또는 파일 경로 지정
- ✅ **리소스 제어**: Driver 및 Executor 메모리/CPU 설정
- ✅ **Iceberg 지원**: Spark를 통한 Iceberg 테이블 처리

## Operator 설명

### 1. KeycloakSparkOperator

Kubernetes Pod에서 Spark 작업을 실행하는 Operator입니다.

**사용 예:**
```python
from airflow_plugins.spark.keycloak_spark_operator import KeycloakSparkOperator

spark_job = KeycloakSparkOperator(
    task_id='run_spark_job',
    keycloak_url='http://keycloak:8080',
    keycloak_realm='spark',
    client_id='spark-client',
    client_secret='your-secret',
    username='spark-user',
    password='your-password',
    spark_app_name='iceberg-etl-job',
    spark_master='kubernetes://https://kubernetes.default.svc',
    spark_image='apache/spark:3.5.0',
    spark_namespace='spark',
    spark_driver_memory='2g',
    spark_executor_memory='2g',
    spark_executor_cores=2,
    spark_executor_instances=3,
    python_script='''
from pyspark.sql import SparkSession
from pyiceberg.catalog import load_catalog

# SparkSession 생성
spark = SparkSession.builder.appName("IcebergETL").getOrCreate()

# Iceberg Catalog 로드
catalog = load_catalog(
    name="iceberg",
    **{"uri": "http://iceberg-rest:8181"}
)

# Iceberg 테이블 읽기
df = spark.table("iceberg.analytics.employees")

# 데이터 처리
result = df.filter(df.salary > 50000).groupBy("department").agg({"salary": "avg"})

# 결과 저장
result.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.analytics.salary_summary")

spark.stop()
    ''',
)
```

---

### 2. KeycloakSparkSubmitOperator

간단한 Spark Submit을 위한 Operator입니다.

**사용 예:**
```python
from airflow_plugins.spark.keycloak_spark_operator import KeycloakSparkSubmitOperator

simple_spark = KeycloakSparkSubmitOperator(
    task_id='simple_spark_job',
    keycloak_url='http://keycloak:8080',
    keycloak_realm='spark',
    client_id='spark-client',
    client_secret='your-secret',
    username='spark-user',
    password='your-password',
    spark_app_name='simple-job',
    python_file='jobs/process_data.py',
)
```

---

## 파라미터 설명

### 필수 파라미터

| 파라미터 | 타입 | 설명 |
|---------|------|------|
| `keycloak_url` | str | Keycloak 서버 URL |
| `keycloak_realm` | str | Keycloak Realm 이름 |
| `client_id` | str | OAuth2 Client ID |
| `username` | str | 인증 사용자명 |
| `password` | str | 인증 비밀번호 |

### 선택 파라미터

| 파라미터 | 타입 | 기본값 | 설명 |
|---------|------|--------|------|
| `client_secret` | str | None | OAuth2 Client Secret |
| `spark_app_name` | str | 'airflow-spark-job' | Spark 애플리케이션 이름 |
| `spark_master` | str | 'kubernetes://https://kubernetes.default.svc' | Spark Master URL |
| `spark_deploy_mode` | str | 'cluster' | 배포 모드 ('cluster' 또는 'client') |
| `spark_image` | str | 'apache/spark:3.5.0' | Spark Docker 이미지 |
| `spark_namespace` | str | 'default' | Kubernetes 네임스페이스 |
| `spark_driver_memory` | str | '2g' | Driver 메모리 |
| `spark_executor_memory` | str | '2g' | Executor 메모리 |
| `spark_executor_cores` | int | 2 | Executor 코어 수 |
| `spark_executor_instances` | int | 2 | Executor 인스턴스 수 |
| `python_script` | str | None | 실행할 Python 스크립트 |
| `python_file` | str | None | 실행할 Python 파일 경로 |
| `py_files` | List[str] | [] | 추가 Python 파일들 |
| `jars` | List[str] | [] | 필요한 JAR 파일들 |
| `packages` | List[str] | [] | Maven 패키지들 |
| `spark_config` | dict | {} | 추가 Spark 설정 |

---

## Keycloak 설정

### 1. Keycloak Realm 생성

```bash
# Keycloak UI에서 Realm 생성
# Realm: spark
```

### 2. Client 생성

```
Client ID: spark-client
Client Protocol: openid-connect
Access Type: confidential
Valid Redirect URIs: *
```

### 3. 사용자 생성

```
Username: spark-user
Password: your-password
Enabled: ON
```

---

## Kubernetes 설정

### 1. Spark ServiceAccount 생성

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark
  namespace: spark
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["*"]
- apiGroups: [""]
  resources: ["pods/exec"]
  verbs: ["*"]
- apiGroups: [""]
  resources: ["pods/log"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark
  namespace: spark
subjects:
- kind: ServiceAccount
  name: spark
  namespace: spark
roleRef:
  kind: Role
  name: spark
  apiGroup: rbac.authorization.k8s.io
```

### 2. Spark Docker 이미지 준비

```dockerfile
FROM apache/spark:3.5.0

# Iceberg Python 패키지 설치
RUN pip install pyiceberg pyspark

# Iceberg Spark Jar 추가
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar /opt/spark/jars/
```

---

## 전체 예제

### 예제 1: Iceberg 테이블 처리

```python
from airflow import DAG
from datetime import datetime, timedelta
from airflow_plugins.spark.keycloak_spark_operator import KeycloakSparkOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_iceberg_job',
    default_args=default_args,
    description='Spark를 사용한 Iceberg 데이터 처리',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'iceberg'],
) as dag:
    
    # Spark 작업
    spark_job = KeycloakSparkOperator(
        task_id='process_iceberg_data',
        keycloak_url='http://keycloak.keycloak.svc.cluster.local:8080',
        keycloak_realm='spark',
        client_id='spark-client',
        client_secret='your-secret',
        username='spark-user',
        password='your-password',
        spark_app_name='iceberg-etl',
        spark_image='your-registry/spark-iceberg:latest',
        spark_namespace='spark',
        spark_executor_instances=3,
        spark_executor_memory='4g',
        python_script='''
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.iceberg.type", "rest") \\
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest.iceberg.svc.cluster.local:8181") \\
    .getOrCreate()

# Iceberg 테이블 읽기
df = spark.read.format("iceberg").table("iceberg.analytics.employees")

# 데이터 처리
result = df.filter(df.salary > 50000)

# 결과를 새 Iceberg 테이블에 저장
result.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.analytics.high_salary_employees")

spark.stop()
        ''',
    )
```

---

### 예제 2: Oracle to Iceberg with Spark

```python
from airflow_plugins.spark.keycloak_spark_operator import KeycloakSparkOperator

oracle_to_iceberg = KeycloakSparkOperator(
    task_id='oracle_to_iceberg_spark',
    keycloak_url='http://keycloak.keycloak.svc.cluster.local:8080',
    keycloak_realm='spark',
    client_id='spark-client',
    client_secret='{{ var.value.spark_client_secret }}',
    username='{{ var.value.spark_username }}',
    password='{{ var.value.spark_password }}',
    spark_app_name='oracle-iceberg-etl',
    spark_image='your-registry/spark-oracle:latest',
    spark_namespace='spark',
    spark_executor_instances=5,
    spark_executor_memory='4g',
    spark_config={
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    },
    python_script='''
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \\
    .config("spark.sql.catalog.iceberg.type", "rest") \\
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \\
    .config("spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:23.3.0.23.09") \\
    .getOrCreate()

# Oracle에서 데이터 읽기
jdbc_url = "jdbc:oracle:thin:@//oracle-svc:1521/orcl"
df = spark.read \\
    .format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", "HR.EMPLOYEES") \\
    .option("user", "hr_user") \\
    .option("password", "hr_password") \\
    .option("driver", "oracle.jdbc.OracleDriver") \\
    .load()

# Iceberg 테이블로 저장
df.write.format("iceberg") \\
    .mode("overwrite") \\
    .saveAsTable("iceberg.analytics.employees")

spark.stop()
    ''',
)
```

---

## 트러블슈팅

### Keycloak 인증 실패

```bash
# Keycloak 서버 연결 확인
curl http://keycloak:8080

# Realm이 존재하는지 확인
curl http://keycloak:8080/realms/spark
```

### Kubernetes Pod 생성 실패

```bash
# ServiceAccount 확인
kubectl get serviceaccount -n spark

# RBAC 확인
kubectl get rolebinding -n spark

# Pod 로그 확인
kubectl logs -n spark <pod-name>
```

### Spark Executor 시작 실패

```bash
# Spark Driver 로그 확인
kubectl logs -n spark <driver-pod>

# Executor Pod 확인
kubectl get pods -n spark
```

---

## 참고

- [Apache Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Keycloak Documentation](https://www.keycloak.org/documentation)
- [Apache Iceberg Spark](https://iceberg.apache.org/docs/latest/spark-ddl/)
- [Airflow Kubernetes Provider](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/)

