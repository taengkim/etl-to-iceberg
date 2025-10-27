"""
Spark Operator 함수 사용 예제

이 파일은 KeycloakSparkOperator에서 python_function 파라미터를 사용하는 예제를 보여줍니다.
"""

from pyspark.sql import SparkSession


def process_iceberg_data():
    """
    Iceberg 데이터를 처리하는 Spark 함수
    
    이 함수는 DAG에서 직접 호출할 수 있습니다.
    """
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest.iceberg.svc.cluster.local:8181") \
        .getOrCreate()
    
    # Iceberg 테이블 읽기
    df = spark.read.format("iceberg").table("iceberg.analytics.employees")
    
    # 데이터 처리
    result = df.filter(df.salary > 50000)
    
    # 결과 저장
    result.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.analytics.high_salary_employees")
    
    spark.stop()


def oracle_to_iceberg():
    """
    Oracle에서 Iceberg로 데이터를 이관하는 함수
    """
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.jars.packages", "com.oracle.database.jdbc:ojdbc8:23.3.0.23.09") \
        .getOrCreate()
    
    # Oracle에서 데이터 읽기
    jdbc_url = "jdbc:oracle:thin:@//oracle-svc:1521/orcl"
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "HR.EMPLOYEES") \
        .option("user", "hr_user") \
        .option("password", "hr_password") \
        .option("driver", "oracle.jdbc.OracleDriver") \
        .load()
    
    # Iceberg 테이블로 저장
    df.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("iceberg.analytics.employees")
    
    spark.stop()


def aggregate_sales_data():
    """
    판매 데이터를 집계하는 함수
    """
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .getOrCreate()
    
    # 판매 데이터 읽기
    sales_df = spark.read.format("iceberg").table("iceberg.analytics.sales")
    
    # 집계 작업
    aggregated = sales_df.groupBy("product_id", "region") \
        .agg({"amount": "sum", "quantity": "sum"}) \
        .withColumnRenamed("sum(amount)", "total_amount") \
        .withColumnRenamed("sum(quantity)", "total_quantity")
    
    # 결과 저장
    aggregated.write.format("iceberg").mode("overwrite").saveAsTable("iceberg.analytics.sales_summary")
    
    spark.stop()

