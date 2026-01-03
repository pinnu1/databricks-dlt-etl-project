import sys
sys.path.append('/Workspace/ETL_project/transformations/Silver_layer/common_utlitiles')

import dlt

# import ONLY your existing utilities
from silver_common_utils import (
    lowercase_columns,
    generic_silver_cleaning
)
def create_basic_silver_table(bronze_stream: str, silver_table: str):

    @dlt.table(
        name=silver_table,
        comment=f"Basic Silver table created from {bronze_stream}"
    )
    def _():

        # Read Bronze streaming data
        df = dlt.read_stream(bronze_stream)

        # Lowercase column names (your function)
        df = lowercase_columns(df)

        #  Apply generic Silver cleaning (your function)
        df = generic_silver_cleaning(df)
        return df

bronze_to_silver = {
    "bronze_address_stream": "silver_address",
    "bronze_customer_stream": "silver_customer",
    "bronze_customeraddress_stream": "silver_customeraddress",
    "bronze_orderheader_stream": "silver_orderheader",
    "bronze_orderdetail_stream": "silver_orderdetail",
    "bronze_productcategory_stream": "silver_productcategory",
    "bronze_product_stream": "silver_product"
}

for bronze, silver in bronze_to_silver.items():
    create_basic_silver_table(bronze, silver)







