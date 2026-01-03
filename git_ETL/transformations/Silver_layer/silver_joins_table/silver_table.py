# import sys
# sys.path.append('/Workspace/ETL_project/transformations/Silver_layer/common_utlitiles')

# from silver_common_utils import *

# import dlt

# def create_silver_table(bronze_stream, silver_name):

#     @dlt.table(
#         name=silver_name,
#         comment=f"Silver table derived from {bronze_stream}"
#     )
#     def _():
#         df = dlt.read_stream(bronze_stream)
#         return generic_silver_cleaning(df)

# bronze_to_silver = {
#     "bronze_address_stream": "silver_address",
#     "bronze_customer_stream": "silver_customer",
#     "bronze_customeraddress_stream": "silver_customeraddress",
#     "bronze_orderdetail_stream": "silver_orderdetail",
#     "bronze_orderheader_stream": "silver_orderheader",
#     "bronze_productcategory_stream": "silver_productcategory"
# }

# for bronze, silver in bronze_to_silver.items():
#     create_silver_table(bronze, silver)