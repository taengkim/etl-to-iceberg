"""
Spark Plugin for Airflow

Kubernetes에서 Spark 작업을 실행하기 위한 Plugin입니다.
"""

from .keycloak_spark_operator import KeycloakSparkOperator

__all__ = ['KeycloakSparkOperator']

