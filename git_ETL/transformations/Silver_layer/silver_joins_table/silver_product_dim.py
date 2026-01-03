import dlt

@dlt.table(
    name="silver_product_dim",
    comment="Product dimension enriched with category"
)
def silver_product_dim():

    product_df = dlt.read_stream("silver_product")
    category_df = dlt.read("silver_productcategory").drop(
        "rowguid", "modifieddate", "silver_processed_ts"
    ).withColumnRenamed("name", "category_name")

    df = product_df.join(
        category_df,
        on="productcategoryid",
        how="left"
    )

    return df