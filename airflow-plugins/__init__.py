from airflow.plugins_manager import AirflowPlugin

# Oracle to Iceberg 플러그인 import
from oracle_to_iceberg.oracle_to_iceberg_operator import OracleToIcebergOperator
from oracle_to_iceberg.oracle_to_iceberg_cdc_operator import OracleToIcebergCDCOperator
from oracle_to_iceberg.hooks import OracleHook

# Iceberg Maintenance 플러그인 import
from maintenance.iceberg_snapshot_operator import IcebergSnapshotOperator
from maintenance.iceberg_compaction_operator import IcebergCompactionOperator
from maintenance.iceberg_aging_operator import IcebergAgingOperator

# Spark 플러그인 import
from spark.keycloak_spark_operator import KeycloakSparkOperator


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
        KeycloakSparkOperator,
    ]
    
    # Plugin에 Hook 등록
    hooks = [
        OracleHook,
    ]
    
    # Airflow UI에서 보일 메뉴 항목 (선택사항)
    flask_blueprints = []
    appbuilder_views = []
    appbuilder_menu_items = []
