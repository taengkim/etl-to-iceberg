from airflow.plugins_manager import AirflowPlugin

# 플러그인 파일들을 import
from oracle_to_iceberg.oracle_to_iceberg_operator import OracleToIcebergOperator
from oracle_to_iceberg.oracle_to_iceberg_cdc_operator import OracleToIcebergCDCOperator
from oracle_to_iceberg.iceberg_snapshot_operator import IcebergSnapshotOperator
from oracle_to_iceberg.iceberg_compaction_operator import IcebergCompactionOperator
from oracle_to_iceberg.iceberg_aging_operator import IcebergAgingOperator
from oracle_to_iceberg.hooks import OracleHook

class OracleToIcebergPlugin(AirflowPlugin):
    """Oracle to Iceberg 데이터 이관 플러그인"""

    name = "oracle_to_iceberg"

    # Plugin에 Operator 등록
    operators = [
        OracleToIcebergOperator,
        OracleToIcebergCDCOperator,
        IcebergSnapshotOperator,
        IcebergCompactionOperator,
        IcebergAgingOperator,
    ]

    # Plugin에 Hook 등록
    hooks = [
        OracleHook,
    ]

    # Airflow UI에서 보일 메뉴 항목 (선택사항)
    flask_blueprints = []
    appbuilder_views = []
    appbuilder_menu_items = []
