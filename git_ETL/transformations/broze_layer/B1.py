import dlt
from pyspark.sql.functions import *

def create_bronze_streaming_view(table_name: str):
    @dlt.view(
        name=f"bronze_{table_name}_stream",
        comment=f"Streaming view for bronze_{table_name}"
    )
    def _view():
        return (
            spark.readStream
            .table(f"etl_catalog.adventureworks.bronze_{table_name}")
        )
bronze_tables = [
    "address",
    "customer",
    "customeraddress",
    "orderdetail",
    "orderheader",
    "productcategory",
    "Product"
]

for tbl in bronze_tables:
    create_bronze_streaming_view(tbl)