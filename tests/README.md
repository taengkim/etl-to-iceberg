# 테스트 가이드

## 테스트 구조

```
tests/
├── conftest.py              # pytest 설정
├── utils/                   # 유틸리티 함수 테스트
│   ├── test_function_converter.py
│   └── test_type_converter.py
└── hdfs_to_iceberg/        # HDFS to Iceberg 테스트
    └── test_hooks.py
```

## 실행 방법

### 모든 테스트 실행
```bash
pytest
```

### 특정 모듈 테스트
```bash
pytest tests/utils/
pytest tests/hdfs_to_iceberg/
```

### 특정 파일 테스트
```bash
pytest tests/utils/test_function_converter.py
```

### 코드 커버리지 포함
```bash
pytest --cov=airflow_plugins --cov-report=html
```

## 테스트 작성 가이드

### 1. 유틸리티 함수 테스트
- 순수 함수의 경우 Mock 없이 직접 테스트
- 예: `test_type_converter.py`, `test_function_converter.py`

### 2. Hook 테스트
- WebHDFS 등의 외부 라이브러리는 Mock 사용
- 예: `test_hooks.py`

### 3. Operator 테스트
- Airflow context와 의존성은 Mock 사용
- PySpark는 실제 세션 생성 대신 검증 로직만 테스트

## 필요한 패키지

테스트를 실행하려면 다음 패키지가 필요합니다:

```bash
pip install pytest pytest-cov pytest-mock
```

