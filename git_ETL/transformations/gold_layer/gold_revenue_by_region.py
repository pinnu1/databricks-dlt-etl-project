import dlt
from pyspark.sql.functions import sum, countDistinct
@dlt.table(
    name="gold_revenue_by_region",
    comment="Revenue and order metrics by customer region"
)
def gold_revenue_by_region():

    #  Read Silver fact
    fact_df = dlt.read("silver_order_product_fact")

    #  Read Silver customer address
    address_df = (
        dlt.read("silver_customer_address")
        .select(
            "customerid",
            "city",
            "stateprovince",
            "countryregion"
        )
    )

    #  Join fact with region info
    df = fact_df.join(
        address_df,
        on="customerid",
        how="left"
    )

    #  Aggregate by region
    gold_df = (
        df.groupBy("countryregion", "stateprovince", "city")
        .agg(
            sum("linetotal").alias("total_revenue"),
            countDistinct("salesorderid").alias("total_orders")
        )
    )

    return gold_df
