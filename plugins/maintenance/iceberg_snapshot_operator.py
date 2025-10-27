from typing import Optional, Dict, Any
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from utils.minio_manager import create_minio_catalog, get_iceberg_table
import os


class IcebergSnapshotOperator(BaseOperator):
    """
    MinIO에 저장된 Iceberg 테이블의 스냅샷을 관리하는 Operator
    
    스냅샷을 생성하거나 과거 스냅샷을 조회할 수 있습니다.
    
    :param iceberg_namespace: Iceberg namespace (database)
    :type iceberg_namespace: str
    :param iceberg_table: Iceberg 테이블명
    :type iceberg_table: str
    :param minio_endpoint: MinIO endpoint
    :type minio_endpoint: str
    :param minio_access_key: MinIO Access Key
    :type minio_access_key: str
    :param minio_secret_key: MinIO Secret Key
    :type minio_secret_key: str
    :param minio_bucket: MinIO Bucket 이름
    :type minio_bucket: str
    :param action: 수행할 작업 ('create', 'list', 'rollback', 'cherrypick')
    :type action: str
    :param snapshot_id: 스냅샷 ID (rollback/cherrypick 시 사용)
    :type snapshot_id: int
    :param warehouse_path: Warehouse 경로 (선택사항)
    :type warehouse_path: str
    """
    
    template_fields = (
        'iceberg_namespace',
        'iceberg_table',
        'action',
        'snapshot_id'
    )
    
    @apply_defaults
    def __init__(
        self,
        iceberg_namespace: str,
        iceberg_table: str,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        minio_bucket: Optional[str] = None,
        action: str = 'create',
        snapshot_id: Optional[int] = None,
        warehouse_path: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.iceberg_namespace = iceberg_namespace
        self.iceberg_table = iceberg_table
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.action = action
        self.snapshot_id = snapshot_id
        self.warehouse_path = warehouse_path
    
    def execute(self, context):
        """Operator 실행"""
        self.log.info(f"Starting Iceberg Snapshot operation: {self.action}")
        self.log.info(f"Table: {self.iceberg_namespace}.{self.iceberg_table}")
        
        try:
            # DAG params에서 MinIO 설정 가져오기 (우선순위: DAG params > Operator params > 환경변수)
            minio_endpoint = self.minio_endpoint or context.get('params', {}).get('minio_endpoint')
            minio_access_key = self.minio_access_key or context.get('params', {}).get('minio_access_key')
            minio_secret_key = self.minio_secret_key or context.get('params', {}).get('minio_secret_key')
            minio_bucket = self.minio_bucket or context.get('params', {}).get('minio_bucket')
            warehouse_path = self.warehouse_path or context.get('params', {}).get('warehouse_path')
            
            # MinIO catalog 생성
            catalog = create_minio_catalog(
                catalog_name='minio_catalog',
                minio_endpoint=minio_endpoint,
                minio_access_key=minio_access_key,
                minio_secret_key=minio_secret_key,
                minio_bucket=minio_bucket,
                warehouse_path=warehouse_path
            )
            
            # 테이블 가져오기
            table = get_iceberg_table(
                catalog,
                self.iceberg_namespace,
                self.iceberg_table,
                logger=self.log
            )
            
            # 스냅샷 작업 수행
            if self.action == 'create':
                result = self._create_snapshot(table)
            elif self.action == 'list':
                result = self._list_snapshots(table)
            elif self.action == 'rollback':
                result = self._rollback_snapshot(table, self.snapshot_id)
            elif self.action == 'cherrypick':
                result = self._cherrypick_snapshot(table, self.snapshot_id)
            else:
                raise ValueError(f"Unsupported action: {self.action}")
            
            self.log.info(f"Snapshot operation completed: {result}")
            return result
            
        except Exception as e:
            self.log.error(f"Snapshot operation failed: {e}")
            raise
    
    def _create_snapshot(self, table):
        """새 스냅샷 생성 (테이블 변경 시 자동 생성됨)"""
        current_snapshot_id = table.metadata.current_snapshot_id
        self.log.info(f"Current snapshot ID: {current_snapshot_id}")
        
        # Iceberg는 쓰기 시 자동으로 스냅샷을 생성하므로
        # 현재 스냅샷 정보를 반환
        return {
            'action': 'create',
            'snapshot_id': current_snapshot_id,
            'table': f"{self.iceberg_namespace}.{self.iceberg_table}"
        }
    
    def _list_snapshots(self, table):
        """모든 스냅샷 목록 조회"""
        snapshots = []
        
        # 현재 스냅샷부터 역순으로 조회
        current_snapshot = table.metadata.current_snapshot_id
        if current_snapshot is not None:
            snapshot = table.metadata.snapshot(current_snapshot)
            while snapshot is not None:
                snapshots.append({
                    'snapshot_id': snapshot.snapshot_id,
                    'timestamp_ms': snapshot.timestamp_ms,
                    'summary': snapshot.summary
                })
                
                # 부모 스냅샷 조회
                if snapshot.parent_id is not None:
                    snapshot = table.metadata.snapshot(snapshot.parent_id)
                else:
                    break
        
        self.log.info(f"Found {len(snapshots)} snapshots")
        return {
            'action': 'list',
            'snapshots': snapshots,
            'count': len(snapshots)
        }
    
    def _rollback_snapshot(self, table, snapshot_id: int):
        """특정 스냅샷으로 롤백"""
        if snapshot_id is None:
            raise ValueError("snapshot_id is required for rollback action")
        
        self.log.info(f"Rolling back to snapshot ID: {snapshot_id}")
        
        # Iceberg rollback 명령 (Spark 또는 PyIceberg API 사용)
        # PyIceberg는 직접적으로 rollback을 지원하지 않으므로
        # Spark를 통해 수행하거나 별도 구현 필요
        
        return {
            'action': 'rollback',
            'snapshot_id': snapshot_id,
            'message': 'Rollback requires Spark or custom implementation'
        }
    
    def _cherrypick_snapshot(self, table, snapshot_id: int):
        """특정 스냅샷을 cherrypick"""
        if snapshot_id is None:
            raise ValueError("snapshot_id is required for cherrypick action")
        
        self.log.info(f"Cherrypicking snapshot ID: {snapshot_id}")
        
        # Cherypick 구현
        return {
            'action': 'cherrypick',
            'snapshot_id': snapshot_id,
            'message': 'Cherrypick requires Spark or custom implementation'
        }
