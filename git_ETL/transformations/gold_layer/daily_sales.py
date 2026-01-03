import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="gold_daily_sales",
    comment="Daily sales KPIs aggregated from order-product fact"
)
def gold_daily_sales():

    df = dlt.read("silver_order_product_fact")

    gold_df = (
        df.groupBy("orderdate")
        .agg(
            sum("linetotal").alias("daily_revenue"),
            sum("orderqty").alias("total_quantity_sold"),
            countDistinct("salesorderid").alias("total_orders")
        )
    )

    return gold_df

@dlt.table(
    name="gold_sales_by_category",
    comment="Revenue and sales metrics by product category"
)
def gold_sales_by_category():

    df = dlt.read("silver_order_product_fact")

    gold_df = (
        df.groupBy("productid")
        .agg(
            sum("linetotal").alias("category_revenue"),
            sum("orderqty").alias("total_quantity_sold"),
            countDistinct("salesorderid").alias("total_orders")
        )
    )

    return gold_df



