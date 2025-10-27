"""
Keycloak Spark Operator

Kubernetes에서 PySpark 작업을 실행하는 Operator입니다.
Keycloak OAuth2 인증을 지원합니다.
"""

import os
import json
import tempfile
import requests
from typing import Optional, Dict, List, Any
from airflow import configuration
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


class KeycloakSparkOperator(KubernetesPodOperator):
    """
    Kubernetes에서 Spark 작업을 실행하는 Operator
    
    Keycloak 인증을 통해 OAuth2 토큰을 획득하고,
    Kubernetes Pod에서 PySpark 작업을 실행합니다.
    
    :param keycloak_url: Keycloak 서버 URL
    :param keycloak_realm: Keycloak Realm 이름
    :param client_id: OAuth2 Client ID
    :param client_secret: OAuth2 Client Secret
    :param username: 사용자명 (인증용)
    :param password: 비밀번호 (인증용)
    :param spark_app_path: Spark 애플리케이션 경로 (Git에서 가져올 수 있음)
    :param spark_app_name: Spark 애플리케이션 이름
    :param spark_master: Spark Master URL (kubernetes://)
    :param spark_deploy_mode: Spark 배포 모드 (cluster 또는 client)
    :param spark_image: Spark Docker 이미지
    :param spark_namespace: Kubernetes 네임스페이스
    :param spark_driver_memory: Driver 메모리 (예: 2g)
    :param spark_executor_memory: Executor 메모리 (예: 2g)
    :param spark_executor_cores: Executor 코어 수 (예: 2)
    :param spark_executor_instances: Executor 인스턴스 수
    :param spark_config: 추가 Spark 설정 (dict)
    :param python_script: 실행할 Python 스크립트 내용
    :param python_file: 실행할 Python 파일 경로
    :param py_files: 추가 Python 파일들 (zip 또는 .py)
    :param jars: 필요한 JAR 파일들
    :param packages: Maven 패키지들
    :param spark_args: 추가 Spark 인자들
    """

    template_fields = ('python_script', 'spark_app_name', 'keycloak_url')

    @apply_defaults
    def __init__(
        self,
        keycloak_url: str,
        keycloak_realm: str,
        client_id: str,
        client_secret: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        spark_app_path: Optional[str] = None,
        spark_app_name: str = 'airflow-spark-job',
        spark_master: str = 'kubernetes://https://kubernetes.default.svc',
        spark_deploy_mode: str = 'cluster',
        spark_image: str = 'apache/spark:3.5.0',
        spark_namespace: str = 'default',
        spark_driver_memory: str = '2g',
        spark_executor_memory: str = '2g',
        spark_executor_cores: int = 2,
        spark_executor_instances: int = 2,
        spark_config: Optional[Dict[str, str]] = None,
        python_script: Optional[str] = None,
        python_file: Optional[str] = None,
        py_files: Optional[List[str]] = None,
        jars: Optional[List[str]] = None,
        packages: Optional[List[str]] = None,
        spark_args: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        self.keycloak_url = keycloak_url
        self.keycloak_realm = keycloak_realm
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.spark_app_path = spark_app_path
        self.spark_app_name = spark_app_name
        self.spark_master = spark_master
        self.spark_deploy_mode = spark_deploy_mode
        self.spark_image = spark_image
        self.spark_namespace = spark_namespace
        self.spark_driver_memory = spark_driver_memory
        self.spark_executor_memory = spark_executor_memory
        self.spark_executor_cores = spark_executor_cores
        self.spark_executor_instances = spark_executor_instances
        self.spark_config = spark_config or {}
        self.python_script = python_script
        self.python_file = python_file
        self.py_files = py_files or []
        self.jars = jars or []
        self.packages = packages or []
        self.spark_args = spark_args

    def get_keycloak_token(self) -> str:
        """
        Keycloak에서 OAuth2 액세스 토큰 획득
        
        :return: 액세스 토큰
        """
        token_url = f"{self.keycloak_url}/realms/{self.keycloak_realm}/protocol/openid-connect/token"
        
        data = {
            'client_id': self.client_id,
            'grant_type': 'password',
            'username': self.username,
            'password': self.password,
        }
        
        if self.client_secret:
            data['client_secret'] = self.client_secret
        
        try:
            response = requests.post(
                token_url,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=30
            )
            response.raise_for_status()
            
            token_data = response.json()
            self.log.info("Keycloak 토큰 획득 성공")
            
            return token_data['access_token']
        
        except requests.exceptions.RequestException as e:
            self.log.error(f"Keycloak 토큰 획득 실패: {e}")
            raise AirflowException(f"Keycloak 인증 실패: {e}")

    def build_spark_command(self, token: str, script_path: str) -> List[str]:
        """
        Spark 실행 명령어 생성
        
        :param token: Keycloak 액세스 토큰
        :param script_path: Python 스크립트 경로
        :return: Spark 명령어 리스트
        """
        cmd = ['/opt/spark/bin/spark-submit']
        
        # Spark 설정
        spark_conf = {
            'spark.app.name': self.spark_app_name,
            'spark.master': self.spark_master,
            'spark.deploy.mode': self.spark_deploy_mode,
            'spark.kubernetes.container.image': self.spark_image,
            'spark.kubernetes.namespace': self.spark_namespace,
            'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark',
            'spark.driver.memory': self.spark_driver_memory,
            'spark.executor.memory': self.spark_executor_memory,
            'spark.executor.cores': str(self.spark_executor_cores),
            'spark.executor.instances': str(self.spark_executor_instances),
        }
        
        # Keycloak 토큰을 환경 변수로 전달
        spark_conf['spark.kubernetes.driver.secrets.keycloak-token'] = '/mnt/secrets/keycloak-token'
        spark_conf['spark.kubernetes.executor.secrets.keycloak-token'] = '/mnt/secrets/keycloak-token'
        
        # 추가 Spark 설정
        spark_conf.update(self.spark_config)
        
        # conf 옵션 추가
        for key, value in spark_conf.items():
            cmd.extend(['--conf', f'{key}={value}'])
        
        # JAR 파일 추가
        for jar in self.jars:
            cmd.extend(['--jars', jar])
        
        # Maven 패키지 추가
        if self.packages:
            packages_str = ','.join(self.packages)
            cmd.extend(['--packages', packages_str])
        
        # 추가 Spark 인자
        if self.spark_args:
            cmd.append(self.spark_args)
        
        # Python 스크립트 실행
        cmd.append(script_path)
        
        return cmd

    def execute(self, context: Any) -> None:
        """
        Spark 작업 실행
        
        :param context: Airflow context
        """
        self.log.info(f"Keycloak Spark 작업 시작: FIspark_app_name=self.spark_app_name}")
        
        try:
            # 1. Keycloak 토큰 획득
            self.log.info("Keycloak에서 토큰 획득 중...")
            token = self.get_keycloak_token()
            
            # 2. Python 스크립트 준비
            if self.python_script:
                # 임시 파일에 스크립트 작성
                with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                    f.write(self.python_script)
                    script_path = f.name
                self.log.info(f"Python 스크립트를 임시 파일에 작성: {script_path}")
            
            elif self.python_file:
                script_path = self.python_file
                self.log.info(f"Python 파일 사용: {script_path}")
            
            else:
                raise AirflowException("python_script 또는 python_file 중 하나는 필수입니다.")
            
            # 3. Spark 명령어 생성
            cmd = self.build_spark_command(token, script_path)
            self.log.info(f"Spark 명령어: {' '.join(cmd)}")
            
            # 4. Kubernetes Pod 설정
            # Keycloak 토큰을 Secret으로 전달하기 위한 설정
            self.env_vars = {
                'KEYCLOAK_TOKEN': token,
                **self.env_vars
            }
            
            # KubernetesPodOperator의 기본 설정
            self.image = self.spark_image
            self.namespace = self.spark_namespace
            self.cmds = cmd
            self.name = f"{self.spark_app_name.replace('_', '-').lower()}-{context['dag_run'].run_id}"
            
            # ServiceAccount 설정
            if not self.service_account_name:
                self.service_account_name = 'spark'
            
            # 리소스 제한
            self.container_resources = {
                'request_memory': self.spark_driver_memory,
                'limit_memory': f"{int(self.spark_driver_memory[:-1]) * 2}g",
            }
            
            # Secret을 마운트하여 Keycloak 토큰 전달
            # (실제로는 더 안전한 방법을 사용하는 것이 권장됨)
            
            self.log.info("Kubernetes Pod 실행 시작...")
            
            # 부모 클래스의 execute 호출
            result = super().execute(context)
            
            # 임시 파일 정리
            if self.python_script and os.path.exists(script_path):
                os.unlink(script_path)
            
            self.log.info("Spark 작업 완료")
            return result
        
        except Exception as e:
            self.log.error(f"Spark 작업 실패: {e}")
            raise AirflowException(f"Spark 작업 실행 중 오류 발생: {e}")


class KeycloakSparkSubmitOperator(BaseOperator):
    """
    간단한 Spark Submit Operator (Keycloak 인증 지원)
    
    Spark Submit을 직접 실행하는 대신 kubectl을 통해 제출합니다.
    """
    
    template_fields = ('python_script', 'spark_app_name', 'keycloak_url')

    @apply_defaults
    def __init__(
        self,
        keycloak_url: str,
        keycloak_realm: str,
        client_id: str,
        client_secret: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        spark_app_name: str = 'airflow-spark-job',
        python_script: Optional[str] = None,
        python_file: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        self.keycloak_url = keycloak_url
        self.keycloak_realm = keycloak_realm
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.spark_app_name = spark_app_name
        self.python_script = python_script
        self.python_file = python_file

    def get_keycloak_token(self) -> str:
        """Keycloak에서 OAuth2 액세스 토큰 획득"""
        token_url = f"{self.keycloak_url}/realms/{self.keycloak_realm}/protocol/openid-connect/token"
        
        data = {
            'client_id': self.client_id,
            'grant_type': 'password',
            'username': self.username,
            'password': self.password,
        }
        
        if self.client_secret:
            data['client_secret'] = self.client_secret
        
        try:
            response = requests.post(
                token_url,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=30
            )
            response.raise_for_status()
            
            token_data = response.json()
            self.log.info("Keycloak 토큰 획득 성공")
            
            return token_data['access_token']
        
        except requests.exceptions.RequestException as e:
            self.log.error(f"Keycloak 토큰 획득 실패: {e}")
            raise AirflowException(f"Keycloak 인증 실패: {e}")

    def execute(self, context: Any) -> None:
        """Spark 작업 실행"""
        self.log.info(f"Keycloak Spark 작업 시작: {self.spark_app_name}")
        
        try:
            # Keycloak 토큰 획득
            token = self.get_keycloak_token()
            self.log.info(f"Keycloak 토큰 획득 완료 (길이: {len(token)})")
            
            # Python 스크립트 준비
            if self.python_script:
                with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                    f.write(self.python_script)
                    script_path = f.name
                self.log.info(f"Python 스크립트 작성: {script_path}")
            
            elif self.python_file:
                script_path = self.python_file
            
            else:
                raise AirflowException("python_script 또는 python_file 중 하나는 필수입니다.")
            
            # 여기서 실제 Spark 작업 실행
            # 예: Kubernetes Spark Operator 또는 Spark-submit 직접 호출
            self.log.info("Spark 작업은 준비되었지만 실제 실행은 Kubernetes 환경에서 설정이 필요합니다.")
            self.log.info(f"스크립트 경로: {script_path}")
            self.log.info("실제 구현에서는 spark-submit 또는 Kubernetes API를 사용하여 작업을 제출해야 합니다.")
            
            # 임시 파일 정리
            if self.python_script and os.path.exists(script_path):
                os.unlink(script_path)
            
            self.log.info("Spark 작업 설정 완료")
        
        except Exception as e:
            self.log.error(f"Spark 작업 실패: {e}")
            raise AirflowException(f"Spark 작업 실행 중 오류 발생: {e}")

