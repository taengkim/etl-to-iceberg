"""Iceberg 테이블 유지보수 플러그인"""
from airflow.plugins_manager import AirflowPlugin
from .iceberg_snapshot_operator import IcebergSnapshotOperator
from .iceberg_compaction_operator import IcebergCompactionOperator
from .iceberg_aging_operator import IcebergAgingOperator

class IcebergMaintenancePlugin(AirflowPlugin):
    """Iceberg 테이블 유지보수 플러그인"""

    name = "iceberg_maintenance"

    operators = [
        IcebergSnapshotOperator,
        IcebergCompactionOperator,
        IcebergAgingOperator,
    ]

    hooks = []
    flask_blueprints = []
    appbuilder_views = []
    appbuilder_menu_items = []

