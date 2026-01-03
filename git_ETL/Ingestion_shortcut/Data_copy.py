from pyspark.sql import SparkSession
from azure_sql_shorcut import jdbc_to_bronze_pipeline

spark = SparkSession.builder.getOrCreate()

jdbc_to_bronze_pipeline(
    spark=spark,
    server_name="fabric",                #Enter your server name of Azure SQL
    database_name="db_testing",           # Enter your database name
    source_table="SalesLT.Product",    # Enter table name you want to copy it 
    bronze_table="Product",                # enter name for which you want to save the table for the broze layer 
    catalog="etl_catalog",                  #Enter catalog name 
    schema="adventureworks",               #Enter Schema name in which you want to save the table
    username="",                 #Your sql Server Username 
    password="",          # your sql server password
    incremental_column="ModifiedDate"     # this is behave as water mark column for incremental load
)

display(
    spark.read.table(
        "etl_catalog.adventureworks.bronze_Product"
    )
)
