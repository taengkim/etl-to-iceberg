"""
HDFS Hook 테스트
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from airflow_plugins.hdfs_to_iceberg.hooks import HdfsHook


class TestHdfsHook:
    """HdfsHook 클래스 테스트"""
    
    def setup_method(self):
        """각 테스트 전 설정"""
        self.hook = HdfsHook(hdfs_conn_id='test_hdfs_conn')
    
    def test_init(self):
        """초기화 테스트"""
        assert self.hook.hdfs_conn_id == 'test_hdfs_conn'
    
    @patch('airflow_plugins.hdfs_to_iceberg.hooks.WebHDFS')
    def test_connection(self, mock_webhdfs):
        """연결 테스트"""
        mock_webhdfs.return_value = Mock()
        self.hook.get_conn()
        
        mock_webhdfs.assert_called_once()
    
    @patch('airflow_plugins.hdfs_to_iceberg.hooks.WebHDFS')
    def test_file_exists(self, mock_webhdfs):
        """파일 존재 확인 테스트"""
        mock_client = MagicMock()
        mock_client.status.return_value = {'FileStatus': {'length': 100}}
        mock_webhdfs.return_value = mock_client
        
        result = self.hook.file_exists('/test/file.txt')
        
        assert result is True
    
    @patch('airflow_plugins.hdfs_to_iceberg.hooks.WebHDFS')
    def test_file_not_exists(self, mock_webhdfs):
        """파일 미존재 확인 테스트"""
        mock_client = MagicMock()
        mock_client.status.side_effect = Exception('File not found')
        mock_webhdfs.return_value = mock_client
        
        result = self.hook.file_exists('/test/nonexistent.txt')
        
        assert result is False
    
    @patch('airflow_plugins.hdfs_to_iceberg.hooks.WebHDFS')
    def test_list_files(self, mock_webhdfs):
        """파일 목록 조회 테스트"""
        mock_client = MagicMock()
        mock_client.listdir.return_value = {
            'FileStatuses': {
                'FileStatus': [
                    {'pathSuffix': 'file1.orc', 'type': 'FILE'},
                    {'pathSuffix': 'file2.orc', 'type': 'FILE'},
                ]
            }
        }
        mock_webhdfs.return_value = mock_client
        
        files = self.hook.list_files('/test/dir', recursive=False)
        
        assert len(files) == 2
        assert '/test/dir/file1.orc' in files or 'file1.orc' in files
    
    @patch('airflow_plugins.hdfs_to_iceberg.hooks.WebHDFS')
    def test_get_file_info(self, mock_webhdfs):
        """파일 정보 조회 테스트"""
        mock_client = MagicMock()
        mock_client.status.return_value = {
            'FileStatus': {
                'length': 1024,
                'modificationTime': 1234567890000,
                'pathSuffix': 'test.orc'
            }
        }
        mock_webhdfs.return_value = mock_client
        
        info = self.hook.get_file_info('/test/file.orc')
        
        assert info['size'] == 1024
        assert info['modification_time'] == 1234567890000
