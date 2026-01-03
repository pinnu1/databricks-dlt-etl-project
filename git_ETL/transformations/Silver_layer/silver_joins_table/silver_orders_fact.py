import dlt
from pyspark.sql.functions import col

from silver_common_utils import (
    apply_business_rules,
    add_surrogate_key_from_composite_keys
)

@dlt.table(
    name="silver_orders_fact",
    comment="Orders fact table joined from order header and order detail"
)
def silver_orders_fact():
    # Select columns to avoid duplicates
    header_df = dlt.read_stream("silver_orderheader").drop(
        "rowguid", "modifieddate", "silver_processed_ts"
    )
    detail_df = dlt.read_stream("silver_orderdetail")

    # Join on SalesOrderID
    df = header_df.join(
        detail_df,
        on="salesorderid",
        how="inner"
    )

    df = apply_business_rules(
        df,
        rules=[
            "orderqty > 0",
            "unitprice >= 0",
            "totaldue >= 0"
        ]
    )

    # Add deterministic surrogate key (FACT-level)
    df = add_surrogate_key_from_composite_keys(
        df,
        natural_keys=["salesorderid", "productid"],
        sk_name="order_fact_sk"
    )

    return df


    