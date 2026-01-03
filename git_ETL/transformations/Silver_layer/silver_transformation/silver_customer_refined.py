from pyspark import pipelines as dp
from pyspark.sql.functions import col
import sys
sys.path.append('/Workspace/ETL_project/transformations/Silver_layer/common_utlitiles')
import dlt
from pyspark.sql.functions import col
# reuse ONLY your utilities
from silver_common_utils import (
    mask_pii,
    add_surrogate_key_from_natural_key
)
def apply_customer_refinement(df):
    # Mask PII columns
    df = mask_pii(
        df,
        pii_columns=["phone"]
    )

    # Add surrogate key
    df = add_surrogate_key_from_natural_key(
    df,
    natural_key="customerid",
    sk_name="customer_sk")


    return df

@dlt.table(
    name="silver_customer_refined",
    comment="Customer Silver with quality rules"
)
@dlt.expect_or_drop("customerid_not_null", "customerid IS NOT NULL")
@dlt.expect("email_present", "emailaddress IS NOT NULL")
def silver_customer_refined():

    df = dlt.read_stream("silver_customer")
    df = apply_customer_refinement(df)
    return df

dp.create_streaming_table("silver_customer_scd")

dp.create_auto_cdc_flow(
    target="silver_customer_scd",
    source="silver_customer_refined",
    keys=["customerid"],
    sequence_by=col("modifieddate"),
    stored_as_scd_type=2
)






