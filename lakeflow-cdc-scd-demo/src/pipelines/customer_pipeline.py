
# Databricks runtime imports (available only in Databricks)
# type: ignore
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("updated_at", StringType(), True)
])

# ----------------------------------
# Bronze: File-based ingestion
# ----------------------------------
@dlt.table(
    name="customers_bronze",
    comment="Raw customers from JSON files"
)
def customers_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(customers_schema)
        .load("/mnt/raw/customers")
    )

# ----------------------------------
# Silver
# ----------------------------------
@dlt.table(
    name="customers_silver"
)
@dlt.expect_or_drop(
    "valid_customer_id",
    "customer_id IS NOT NULL"
)
def customers_silver():
    return (
        dlt.read_stream("customers_bronze")
        .withColumn("updated_at", to_timestamp("updated_at"))
        .withColumn("city", upper(col("city")))
    )

# ----------------------------------
# CDC: SCD Type 1
# ----------------------------------
dlt.create_streaming_table("customers_cdc")

dlt.apply_changes(
    target="customers_cdc",
    source="customers_silver",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1
)

# ----------------------------------
# SCD Type 2
# ----------------------------------
dlt.create_streaming_table("customers_dim")

dlt.apply_changes(
    target="customers_dim",
    source="customers_silver",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)

# ----------------------------------
# Gold
# ----------------------------------
@dlt.table
def active_customers_gold():
    return dlt.read("customers_dim").filter(col("__END_AT").isNull())
