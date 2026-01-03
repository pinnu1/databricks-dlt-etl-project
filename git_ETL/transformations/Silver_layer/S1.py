# from pyspark.sql.functions import current_timestamp

# @dlt.table(
#     name="silver_customer",
#     comment="Cleaned customer data (DLT-required table)"
# )
# @dlt.expect("valid_customer_id", "CustomerID IS NOT NULL")
# def silver_customer():
#     return (
#         dlt.read_stream("bronze_customer_stream")
#         .dropDuplicates(["CustomerID"])
#         .withColumn("processed_timestamp", current_timestamp())
#     )
# import sys
# sys.path.append('/Workspace/ETL_project/transformations/Silver_layer/common_utlitiles')