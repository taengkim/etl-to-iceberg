# Utils

공유 유틸리티 모듈 모음입니다.

## 모듈 목록

### 1. spark_builder.py

Spark 설정 및 SparkSession 생성을 위한 유틸리티입니다.

**주요 함수:**
- `build_spark_submit_command()`: Spark submit 명령어 생성
- `build_kubernetes_spark_config()`: Kubernetes Spark 설정 생성
- `build_iceberg_spark_config()`: Iceberg Spark 설정 생성
- `create_iceberg_spark_session()`: Iceberg용 SparkSession 생성

**사용 예:**

```python
from utils.spark_builder import create_iceberg_spark_session

spark = create_iceberg_spark_session(
    app_name='my_app',
    catalog_name='iceberg',
    catalog_uri='http://iceberg-rest:8181',
    warehouse_path='s3://iceberg/warehouse',
)
```

### 2. function_converter.py

Python 함수를 소스 코드 문자열로 변환합니다.

**주요 함수:**
- `function_to_script()`: 함수를 실행 가능한 스크립트로 변환
- `extract_function_dependencies()`: 함수의 의존성 추출
- `create_function_wrapper()`: 함수 래퍼 생성

**사용 예:**

```python
from utils.function_converter import function_to_script

def my_function():
    print("Hello")

script = function_to_script(my_function)
```

### 3. catalog_manager.py

Iceberg Catalog 관리를 위한 유틸리티입니다.

**주요 함수:**
- `create_iceberg_catalog_config()`: Catalog 설정 생성
- `load_or_create_catalog()`: Catalog 로드 또는 생성
- `ensure_namespace()`: Namespace 생성

### 4. minio_manager.py

MinIO Storage 관리를 위한 유틸리티입니다.

**주요 함수:**
- `create_minio_catalog()`: MinIO용 Iceberg Catalog 생성
- `get_iceberg_table()`: Iceberg 테이블 가져오기

### 5. schema_builder.py

Iceberg 스키마 생성을 위한 유틸리티입니다.

### 6. type_converter.py

Oracle → Iceberg 타입 변환을 위한 유틸리티입니다.

### 7. keycloak_auth.py

Keycloak OAuth2 인증을 위한 유틸리티입니다.

### 8. dataframe_utils.py

DataFrame 처리를 위한 유틸리티입니다.

## 참고

- 모든 유틸리티는 재사용 가능하도록 설계되었습니다
- 각 모듈은 독립적으로 사용 가능합니다

