"""
HDFS Hook

HDFS 연결 및 파일 조작을 위한 Hook입니다.
"""

import os
from typing import List, Dict, Optional
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class HdfsHook(BaseHook):
    """
    HDFS 연결 및 파일 조작 Hook
    
    HDFS 클라이언트를 사용하여 HDFS 파일 시스템에 접근합니다.
    """

    def __init__(
        self,
        hdfs_conn_id: str = 'hdfs_default',
        hdfs_host: Optional[str] = None,
        hdfs_port: int = 9000,
        hdfs_user: Optional[str] = None,
        hdfs_password: Optional[str] = None,
    ):
        """
        HDFS Hook 초기화
        
        :param hdfs_conn_id: Airflow Connection ID
        :param hdfs_host: HDFS 네임노드 호스트
        :param hdfs_port: HDFS 네임노드 포트
        :param hdfs_user: HDFS 사용자
        :param hdfs_password: HDFS 비밀번호 (필요시)
        """
        super().__init__()
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.hdfs_user = hdfs_user
        self.hdfs_password = hdfs_password
        self._hdfs_client = None

    def get_conn(self):
        """
        HDFS 클라이언트 생성
        
        :return: HDFS 클라이언트
        """
        if self._hdfs_client is None:
            try:
                # PyHDFS 또는 hdfs3 라이브러리 사용
                try:
                    from hdfs import InsecureClient
                    from hdfs.ext.kerberos import KerberosClient
                except ImportError:
                    raise AirflowException("hdfs 라이브러리가 설치되지 않았습니다. pip install hdfs를 실행하세요.")
                
                # Connection에서 설정 가져오기
                if not self.hdfs_host:
                    conn = self.get_connection(self.hdfs_conn_id)
                    self.hdfs_host = conn.host or 'localhost'
                    self.hdfs_port = conn.port or self.hdfs_port
                    self.hdfs_user = conn.login or self.hdfs_user
                    self.hdfs_password = conn.password or self.hdfs_password
                
                hdfs_url = f'http://{self.hdfs_host}:{self.hdfs_port}'
                
                # Kerberos 인증이 필요한 경우
                if self.hdfs_user and self.hdfs_password:
                    # Kerberos 클라이언트 생성
                    self._hdfs_client = KerberosClient(hdfs_url, user=self.hdfs_user)
                else:
                    # Insecure 클라이언트 생성 (개발/테스트용)
                    self._hdfs_client = InsecureClient(hdfs_url, user=self.hdfs_user or 'hdfs')
                
                self.log.info(f"HDFS 연결 성공: {hdfs_url}")
                
            except Exception as e:
                self.log.error(f"HDFS 연결 실패: {e}")
                raise AirflowException(f"HDFS 연결 실패: {e}")
        
        return self._hdfs_client

    def list_files(self, path: str, recursive: bool = False) -> List[str]:
        """
        HDFS 디렉토리의 파일 목록 조회
        
        :param path: HDFS 경로
        :param recursive: 재귀적으로 조회할지 여부
        :return: 파일 경로 리스트
        """
        try:
            client = self.get_conn()
            files = client.list(path, status=True)
            
            file_paths = []
            for item in files:
                item_path = os.path.join(path, item).replace('\\', '/')
                
                # 재귀적 조회
                if recursive and item.get('type') == 'DIRECTORY':
                    file_paths.extend(self.list_files(item_path, recursive=True))
                elif item.get('type') == 'FILE':
                    file_paths.append(item_path)
            
            return file_paths
        
        except Exception as e:
            self.log.error(f"파일 목록 조회 실패: {path}, {e}")
            raise AirflowException(f"파일 목록 조회 실패: {e}")

    def get_file_info(self, path: str) -> Dict:
        """
        파일 정보 조회 (크기, 수정 시간 등)
        
        :param path: HDFS 파일 경로
        :return: 파일 정보 딕셔너리
        """
        try:
            client = self.get_conn()
            status = client.status(path)
            
            return {
                'path': path,
                'size': status.get('length', 0),
                'modification_time': status.get('modificationTime', 0),
                'block_size': status.get('blockSize', 0),
                'owner': status.get('owner', ''),
                'group': status.get('group', ''),
            }
        
        except Exception as e:
            self.log.error(f"파일 정보 조회 실패: {path}, {e}")
            raise AirflowException(f"파일 정보 조회 실패: {e}")

    def read_file(self, path: str) -> bytes:
        """
        HDFS 파일 읽기
        
        :param path: HDFS 파일 경로
        :return: 파일 내용 (bytes)
        """
        try:
            client = self.get_conn()
            
            with client.read(path) as reader:
                return reader.read()
        
        except Exception as e:
            self.log.error(f"파일 읽기 실패: {path}, {e}")
            raise AirflowException(f"파일 읽기 실패: {e}")

    def download_file(self, hdfs_path: str, local_path: str):
        """
        HDFS 파일을 로컬로 다운로드
        
        :param hdfs_path: HDFS 파일 경로
        :param local_path: 로컬 저장 경로
        """
        try:
            client = self.get_conn()
            client.download(hdfs_path, local_path)
            self.log.info(f"파일 다운로드 완료: {hdfs_path} -> {local_path}")
        
        except Exception as e:
            self.log.error(f"파일 다운로드 실패: {hdfs_path}, {e}")
            raise AirflowException(f"파일 다운로드 실패: {e}")

    def file_exists(self, path: str) -> bool:
        """
        파일 존재 여부 확인
        
        :param path: HDFS 파일 경로
        :return: 파일 존재 여부
        """
        try:
            client = self.get_conn()
            return client.status(path) is not None
        except:
            return False

