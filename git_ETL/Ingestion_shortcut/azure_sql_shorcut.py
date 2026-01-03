# =========================
# JDBC → BRONZE UTILITIES
# =========================

from pyspark.sql import SparkSession


# -------------------------------------------------
# 1. READ FROM AZURE SQL (FULL / INCREMENTAL)
# -------------------------------------------------
def read_from_azure_sql_jdbc(
    spark,
    server_name: str,
    database_name: str,
    table_name: str,
    username: str,
    password: str,
    incremental_column: str = None,
    last_loaded_value=None
):
    """
    Generic JDBC reader for Azure SQL.
    Supports full load and incremental load.
    """

    jdbc_url = (
        f"jdbc:sqlserver://{server_name}.database.windows.net:1433;"
        f"database={database_name};"
        "encrypt=true;"
        "trustServerCertificate=false;"
        "loginTimeout=30;"
    )

    if incremental_column and last_loaded_value:
        query = (
            f"(SELECT * FROM {table_name} "
            f"WHERE {incremental_column} > '{last_loaded_value}') AS src"
        )
    else:
        query = table_name

    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", username)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )

    return df


# -------------------------------------------------
# 2. GET LAST LOADED VALUE FROM BRONZE
# -------------------------------------------------
def get_last_loaded_value(
    spark,
    catalog: str,
    schema: str,
    bronze_table: str,
    incremental_column: str
):
    """
    Fetch max incremental column value from Bronze table
    """

    table_full_name = f"{catalog}.{schema}.bronze_{bronze_table}"

    if spark.catalog.tableExists(table_full_name):
        return (
            spark.sql(
                f"SELECT MAX({incremental_column}) AS max_val "
                f"FROM {table_full_name}"
            )
            .collect()[0]["max_val"]
        )
    else:
        return None


# -------------------------------------------------
# 3. WRITE TO BRONZE (UNITY CATALOG)
# -------------------------------------------------
def write_to_bronze_table(
    df,
    catalog: str,
    schema: str,
    table_name: str,
    mode: str = "append"
):
    """
    Writes data to Unity Catalog Bronze Delta table
    """

    (
        df.write
        .format("delta")
        .mode(mode)
        .saveAsTable(f"{catalog}.{schema}.bronze_{table_name}")
    )


# -------------------------------------------------
# 4. END-TO-END JDBC → BRONZE PIPELINE
# -------------------------------------------------
def jdbc_to_bronze_pipeline(
    spark,
    server_name: str,
    database_name: str,
    source_table: str,
    bronze_table: str,
    catalog: str,
    schema: str,
    username: str,
    password: str,
    incremental_column: str = None
):
    """
    End-to-end JDBC to Bronze ingestion with incremental support
    """

    last_value = None

    if incremental_column:
        last_value = get_last_loaded_value(
            spark=spark,
            catalog=catalog,
            schema=schema,
            bronze_table=bronze_table,
            incremental_column=incremental_column
        )

    df = read_from_azure_sql_jdbc(
        spark=spark,
        server_name=server_name,
        database_name=database_name,
        table_name=source_table,
        username=username,
        password=password,
        incremental_column=incremental_column,
        last_loaded_value=last_value
    )

    write_to_bronze_table(
        df=df,
        catalog=catalog,
        schema=schema,
        table_name=bronze_table,
        mode="append"
    )
