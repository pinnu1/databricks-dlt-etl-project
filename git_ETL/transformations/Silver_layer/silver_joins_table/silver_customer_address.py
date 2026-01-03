import dlt

@dlt.table(
    name="silver_customer_address",
    comment="Customer master with address information"
)
def silver_customer_address():

    # STREAM: Customer can change
    customer_df = dlt.read_stream("silver_customer")

    # BATCH: mapping table (dimension-style)
    cust_addr_df = (
        dlt.read("silver_customeraddress")
        .drop("rowguid", "modifieddate", "silver_processed_ts")
    )

    # BATCH: address dimension
    address_df = (
        dlt.read("silver_address")
        .drop("rowguid", "modifieddate", "silver_processed_ts")
    )

    df = (
        customer_df
        .join(cust_addr_df, on="customerid", how="left")
        .join(address_df, on="addressid", how="left")
    )

    return df
