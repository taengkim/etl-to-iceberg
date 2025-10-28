"""
Spark 명령어 빌더 유틸리티

Spark submit 명령어를 생성하는 공통 함수들입니다.
"""

from typing import Dict, List, Optional, Any


def build_spark_submit_command(
    script_path: str,
    app_name: str,
    master: str = 'local[*]',
    deploy_mode: str = 'client',
    spark_config: Optional[Dict[str, str]] = None,
    jars: Optional[List[str]] = None,
    packages: Optional[List[str]] = None,
    py_files: Optional[List[str]] = None,
    files: Optional[List[str]] = None,
    archives: Optional[List[str]] = None,
    spark_args: Optional[str] = None,
    driver_memory: Optional[str] = None,
    executor_memory: Optional[str] = None,
    executor_cores: Optional[int] = None,
    executor_instances: Optional[int] = None,
    kubernetes_image: Optional[str] = None,
    kubernetes_namespace: Optional[str] = None,
    kubernetes_service_account: str = 'spark'
) -> List[str]:
    """
    Spark Submit 명령어 생성
    
    :param script_path: 실행할 Python 스크립트 경로
    :param app_name: Spark 애플리케이션 이름
    :param master: Spark Master URL
    :param deploy_mode: 배포 모드 ('client' 또는 'cluster')
    :param spark_config: 추가 Spark 설정 (dict)
    :param jars: 필요한 JAR 파일들
    :param packages: Maven 패키지들
    :param py_files: 추가 Python 파일들
    :param files: Spark에서 사용할 파일들
    :param archives: 압축 파일들 (zip, tar.gz 등)
    :param spark_args: 추가 Spark 인자들
    :param driver_memory: Driver 메모리 (예: '2g')
    :param executor_memory: Executor 메모리 (예: '2g')
    :param executor_cores: Executor 코어 수
    :param executor_instances: Executor 인스턴스 수
    :param kubernetes_image: Kubernetes 이미지
    :param kubernetes_namespace: Kubernetes 네임스페이스
    :param kubernetes_service_account: Kubernetes Service Account
    :return: Spark Submit 명령어 리스트
    """
    cmd = ['spark-submit']
    
    # 기본 Spark 설정
    spark_conf = {
        'spark.app.name': app_name,
        'spark.master': master,
        'spark.deploy.mode': deploy_mode,
    }
    
    # 메모리 및 리소스 설정
    if driver_memory:
        spark_conf['spark.driver.memory'] = driver_memory
    
    if executor_memory:
        spark_conf['spark.executor.memory'] = executor_memory
    
    if executor_cores:
        spark_conf['spark.executor.cores'] = str(executor_cores)
    
    if executor_instances:
        spark_conf['spark.executor.instances'] = str(executor_instances)
    
    # Kubernetes 설정
    if kubernetes_image:
        spark_conf['spark.kubernetes.container.image'] = kubernetes_image
    
    if kubernetes_namespace:
        spark_conf['spark.kubernetes.namespace'] = kubernetes_namespace
    
    spark_conf['spark.kubernetes.authenticate.driver.serviceAccountName'] = kubernetes_service_account
    
    # 추가 Spark 설정
    if spark_config:
        spark_conf.update(spark_config)
    
    # conf 옵션 추가
    for key, value in spark_conf.items():
        cmd.extend(['--conf', f'{key}={value}'])
    
    # JAR 파일 추가
    if jars:
        for jar in jars:
            cmd.extend(['--jars', jar])
    
    # Maven 패키지 추가
    if packages:
        packages_str = ','.join(packages)
        cmd.extend(['--packages', packages_str])
    
    # Python 파일 추가
    if py_files:
        py_files_str = ','.join(py_files)
        cmd.extend(['--py-files', py_files_str])
    
    # 파일 추가
    if files:
        files_str = ','.join(files)
        cmd.extend(['--files', files_str])
    
    # 아카이브 추가
    if archives:
        archives_str = ','.join(archives)
        cmd.extend(['--archives', archives_str])
    
    # 추가 Spark 인자
    if spark_args:
        cmd.append(spark_args)
    
    # Python 스크립트 실행
    cmd.append(script_path)
    
    return cmd


def build_kubernetes_spark_config(
    image: str,
    namespace: str,
    service_account: str = 'spark',
    driver_cores: Optional[int] = None,
    driver_memory: Optional[str] = None,
    executor_cores: Optional[int] = None,
    executor_memory: Optional[str] = None,
    executor_instances: Optional[int] = None,
    executor_request_cores: Optional[int] = None,
    additional_config: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """
    Kubernetes용 Spark 설정 생성
    
    :param image: Spark 이미지
    :param namespace: Kubernetes 네임스페이스
    :param service_account: Service Account 이름
    :param driver_cores: Driver 코어 수
    :param driver_memory: Driver 메모리
    :param executor_cores: Executor 코어 수
    :param executor_memory: Executor 메모리
    :param executor_instances: Executor 인스턴스 수
    :param executor_request_cores: Executor 요청 코어 수
    :param additional_config: 추가 설정
    :return: Spark 설정 딕셔너리
    """
    config = {
        'spark.kubernetes.container.image': image,
        'spark.kubernetes.namespace': namespace,
        'spark.kubernetes.authenticate.driver.serviceAccountName': service_account,
    }
    
    if driver_cores:
        config['spark.kubernetes.driver.request.cores'] = str(driver_cores)
    
    if driver_memory:
        config['spark.kubernetes.driver.memory'] = driver_memory
    
    if executor_cores:
        config['spark.executor.cores'] = str(executor_cores)
    
    if executor_memory:
        config['spark.executor.memory'] = executor_memory
    
    if executor_instances:
        config['spark.executor.instances'] = str(executor_instances)
    
    if executor_request_cores:
        config['spark.kubernetes.executor.request.cores'] = str(executor_request_cores)
    
    if additional_config:
        config.update(additional_config)
    
    return config


def build_iceberg_spark_config(
    catalog_name: str = 'iceberg',
    catalog_uri: Optional[str] = None,
    warehouse_path: Optional[str] = None,
    additional_packages: Optional[List[str]] = None
) -> Dict[str, str]:
    """
    Iceberg용 Spark 설정 생성
    
    :param catalog_name: Catalog 이름
    :param catalog_uri: Iceberg REST Catalog URI
    :param warehouse_path: Iceberg Warehouse 경로
    :param additional_packages: 추가 Maven 패키지
    :return: Spark 설정 딕셔너리
    """
    config = {
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        f'spark.sql.catalog.{catalog_name}': 'org.apache.iceberg.spark.SparkCatalog',
    }
    
    if catalog_uri:
        config[f'spark.sql.catalog.{catalog_name}.type'] = 'rest'
        config[f'spark.sql.catalog.{catalog_name}.uri'] = catalog_uri
    
    if warehouse_path:
        config[f'spark.sql.catalog.{catalog_name}.warehouse'] = warehouse_path
    
    return config


def create_iceberg_spark_session(
    app_name: str,
    catalog_name: str = 'iceberg',
    catalog_uri: Optional[str] = None,
    warehouse_path: Optional[str] = None,
    additional_config: Optional[Dict[str, str]] = None,
    master: Optional[str] = None
) -> Any:
    """
    Iceberg용 SparkSession 생성
    
    :param app_name: Spark 애플리케이션 이름
    :param catalog_name: Catalog 이름
    :param catalog_uri: Iceberg REST Catalog URI
    :param warehouse_path: Iceberg Warehouse 경로
    :param additional_config: 추가 Spark 설정
    :param master: Spark Master URL (기본값: None)
    :return: SparkSession
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        raise ImportError("PySpark가 설치되지 않았습니다.")
    
    # Iceberg 설정 생성
    iceberg_config = build_iceberg_spark_config(
        catalog_name=catalog_name,
        catalog_uri=catalog_uri,
        warehouse_path=warehouse_path
    )
    
    # 기본 설정
    spark_config = {
        'spark.app.name': app_name,
        'spark.sql.parquet.compression.codec': 'zstd',
        **iceberg_config
    }
    
    # 추가 설정 적용
    if additional_config:
        spark_config.update(additional_config)
    
    # SparkSession 빌더 생성
    spark_builder = SparkSession.builder.appName(app_name)
    
    # Master 설정
    if master:
        spark_builder = spark_builder.master(master)
    
    # 모든 설정 적용
    for key, value in spark_config.items():
        spark_builder = spark_builder.config(key, value)
    
    # SparkSession 생성
    spark = spark_builder.getOrCreate()
    
    return spark

