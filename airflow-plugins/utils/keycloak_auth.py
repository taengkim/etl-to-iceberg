"""
Keycloak 인증 유틸리티

Keycloak OAuth2 인증을 위한 공통 함수들입니다.
"""

import requests
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin


def get_keycloak_token(
    keycloak_url: str,
    keycloak_realm: str,
    client_id: str,
    client_secret: str = None,
    username: str = None,
    password: str = None,
    logger: LoggingMixin = None
) -> str:
    """
    Keycloak에서 OAuth2 액세스 토큰 획득
    
    :param keycloak_url: Keycloak 서버 URL
    :param keycloak_realm: Keycloak Realm 이름
    :param client_id: OAuth2 Client ID
    :param client_secret: OAuth2 Client Secret (선택사항)
    :param username: 사용자명 (인증용)
    :param password: 비밀번호 (인증용)
    :param logger: 로깅을 위한 Airflow Logger (선택사항)
    :return: 액세스 토큰
    """
    token_url = f"{keycloak_url}/realms/{keycloak_realm}/protocol/openid-connect/token"
    
    data = {
        'client_id': client_id,
        'grant_type': 'password',
        'username': username,
        'password': password,
    }
    
    if client_secret:
        data['client_secret'] = client_secret
    
    try:
        if logger:
            logger.info(f"Keycloak 토큰 요청: {keycloak_url}/realms/{keycloak_realm}")
        
        response = requests.post(
            token_url,
            data=data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30
        )
        response.raise_for_status()
        
        token_data = response.json()
        
        if logger:
            logger.info("Keycloak 토큰 획득 성공")
        
        return token_data['access_token']
    
    except requests.exceptions.RequestException as e:
        error_msg = f"Keycloak 토큰 획득 실패: {e}"
        
        if logger:
            logger.error(error_msg)
        
        raise AirflowException(error_msg)


def refresh_keycloak_token(
    keycloak_url: str,
    keycloak_realm: str,
    client_id: str,
    refresh_token: str,
    client_secret: str = None,
    logger: LoggingMixin = None
) -> str:
    """
    Keycloak 리프레시 토큰으로 새 액세스 토큰 획득
    
    :param keycloak_url: Keycloak 서버 URL
    :param keycloak_realm: Keycloak Realm 이름
    :param client_id: OAuth2 Client ID
    :param refresh_token: 리프레시 토큰
    :param client_secret: OAuth2 Client Secret (선택사항)
    :param logger: 로깅을 위한 Airflow Logger (선택사항)
    :return: 새 액세스 토큰
    """
    token_url = f"{keycloak_url}/realms/{keycloak_realm}/protocol/openid-connect/token"
    
    data = {
        'client_id': client_id,
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
    }
    
    if client_secret:
        data['client_secret'] = client_secret
    
    try:
        if logger:
            logger.info("Keycloak 토큰 갱신 요청")
        
        response = requests.post(
            token_url,
            data=data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30
        )
        response.raise_for_status()
        
        token_data = response.json()
        
        if logger:
            logger.info("Keycloak 토큰 갱신 성공")
        
        return token_data['access_token']
    
    except requests.exceptions.RequestException as e:
        error_msg = f"Keycloak 토큰 갱신 실패: {e}"
        
        if logger:
            logger.error(error_msg)
        
        raise AirflowException(error_msg)

