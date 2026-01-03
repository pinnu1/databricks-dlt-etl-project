import dlt
from pyspark.sql.functions import col
import sys
sys.path.append('/Workspace/ETL_project/transformations/Silver_layer/common_utlitiles')
from silver_common_utils import (
    apply_business_rules,
    add_surrogate_key_from_composite_keys
)

@dlt.table(
    name="silver_order_product_fact",
    comment="Order-Product line fact with data quality rules"
)
@dlt.expect_or_drop("valid_orderqty", "orderqty > 0")
@dlt.expect_or_drop("valid_unitprice", "unitprice >= 0")
@dlt.expect_or_drop("valid_linetotal", "linetotal >= 0")
@dlt.expect_or_drop("salesorderid_not_null", "salesorderid IS NOT NULL")
@dlt.expect_or_drop("productid_not_null", "productid IS NOT NULL")
def silver_order_product_fact():

    header_df = (
        dlt.read_stream("silver_orderheader")
        .drop("rowguid", "modifieddate", "silver_processed_ts")
        .alias("h")
    )

    detail_df = (
        dlt.read_stream("silver_orderdetail")
        .drop("rowguid", "modifieddate", "silver_processed_ts")
        .alias("d")
    )

    df = header_df.join(
        detail_df,
        col("h.salesorderid") == col("d.salesorderid"),
        "inner"
    ).select(
        col("h.salesorderid"),
        col("d.productid"),
        col("h.customerid"),
        col("h.orderdate"),
        col("d.orderqty"),
        col("d.unitprice"),
        col("d.unitpricediscount"),
        col("d.linetotal"),
        col("h.totaldue")
    )

    df = add_surrogate_key_from_composite_keys(
        df,
        natural_keys=["salesorderid", "productid"],
        sk_name="order_product_sk"
    )

    return df
