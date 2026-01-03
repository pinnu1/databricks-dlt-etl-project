import dlt
from pyspark.sql.functions import sum, countDistinct, expr,col
@dlt.table(
    name="gold_customer_sales_profile",
    comment="Customer sales KPIs enriched with customer name and email"
)
def gold_customer_sales_profile():

    # Read Silver fact table
    fact_df = dlt.read("silver_order_product_fact")

    # Aggregate to customer-level KPIs
    kpi_df = (
        fact_df.groupBy("customerid")
        .agg(
            sum("linetotal").alias("total_revenue"),
            sum("orderqty").alias("total_quantity"),
            countDistinct("salesorderid").alias("total_orders")
        )
        .withColumn(
            "avg_order_value",
            expr("total_revenue / total_orders")
        )
    )

    # Read customer attributes from refined Silver
    customer_df = (
        dlt.read("silver_customer_refined")
        .select(
            "customerid",
            "firstname",
            "lastname",
            "emailaddress"
        )
    )

    # Join KPIs with customer attributes
    df = kpi_df.join(
        customer_df,
        on="customerid",
        how="left"
    )

    # Create display-friendly customer name
    df = df.withColumn(
        "customer_name",
        col("firstname") + " " + col("lastname")
    )

    return df
