# Databricks runtime imports (available only in Databricks)
# type: ignore

import dlt
from pyspark.sql.functions import *

# ----------------------------------
# JDBC Configuration (Azure SQL)
# ----------------------------------

# ---------------------------
# Secrets
# ---------------------------
sql_host = dbutils.secrets.get("dab-sql-server", "sql_host")
sql_user = dbutils.secrets.get("dab-sql-server", "sql_user")
sql_password = dbutils.secrets.get("dab-sql-server", "sql_password")
sql_database = "lfdemo"

jdbc_url = (
    f"jdbc:sqlserver://{sql_host}:1433;"
    f"databaseName={sql_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30;"
)
 
print(f"JDBC URL configured is {jdbc_url}.")
jdbc_properties = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

  
# ----------------------------------
# Bronze: Azure SQL Batch Ingestion
# ----------------------------------
@dlt.table(
    name="customers_bronze",
    comment="Raw customers from Azure SQL"
)
def customers_bronze():
    return (
        spark.read
        .jdbc(
            url=jdbc_url,
            table="demo.customers",
            properties=jdbc_properties
        )
    )

# ----------------------------------
# Silver: Incremental CDC
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
        dlt.read("customers_bronze")
        .withColumn("updated_at", to_timestamp("updated_at"))
        .withColumn("city", upper(col("city")))
    )

# ----------------------------------
# CDC: SCD Type 1
# ----------------------------------
dlt.create_streaming_table(
    name="customers_cdc"
)

dlt.apply_changes(
    target="customers_cdc",
    source="customers_silver",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1
)

# ----------------------------------
# SCD Type 2 Dimension
# ----------------------------------
dlt.create_streaming_table(
    name="customers_dim"
)

dlt.apply_changes(
    target="customers_dim",
    source="customers_silver",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)

# ----------------------------------
# Gold: Active Customers
# ----------------------------------
@dlt.table(
    name="active_customers_gold",
    comment="Current active customers"
)
def active_customers_gold():
    return (
        dlt.read("customers_dim")
        .filter(col("__END_AT").isNull())
    )
