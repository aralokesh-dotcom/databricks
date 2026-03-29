# Import DLT library
import dlt
from pyspark.sql.functions import *

# ======================================================================
# BRONZE LAYER - Raw Data Ingestion
# ======================================================================

# Bronze: Ingest raw JSON files using Auto Loader
@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from source",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/Volumes/main/default/raw_data_volume/exported_users_json/")
            .withColumn("ingestion_time", current_timestamp())
    )


# ======================================================================
# SILVER LAYER - Cleaned and Validated
# ======================================================================

# Silver: Clean data with quality checks
@dlt.table(
    name="silver_customers",
    comment="Cleaned customer data with quality checks",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "name IS NOT NULL")
@dlt.expect("recent_data", "ingestion_time > current_date() - INTERVAL 30 DAYS")
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
            .select(
                col("id").alias("customer_id"),
                col("name"),
                col("info"),
                col("date"),
                col("active"),
                col("amount"),
                col("ingestion_time")
            )
            .dropDuplicates(["customer_id"])
    )

# ======================================================================
# GOLD LAYER - Business Aggregations
# ======================================================================

# Gold: Business-level aggregations
@dlt.table(
    name="gold_customers_summary",
    comment="Customer aggregations - analytics ready",
    table_properties={"quality": "gold"}
)
def gold_customers_summary():
    return (
        dlt.read("silver_customers")
            .groupBy("active")
            .agg(
                count("customer_id").alias("customer_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                min("date").alias("first_date"),
                max("date").alias("last_date")
            )
    )

# ======================================================================
# EXAMPLE: Additional Bronze Source (commented out - no data yet)
# ======================================================================

# Uncomment when you have orders data in /Volumes/main/default/raw_data_volume/orders/
#
# @dlt.table(name="bronze_orders")
# def bronze_orders():
#     return (
#         spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "json")
#             .load("/Volumes/main/default/raw_data_volume/orders/")
#             .withColumn("ingestion_time", current_timestamp())
#     )
#
# @dlt.table(name="silver_orders")
# @dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
# @dlt.expect_or_drop("positive_amount", "amount > 0")
# def silver_orders():
#     return (
#         dlt.read_stream("bronze_orders")
#             .select(
#                 col("order_id").cast("integer"),
#                 col("customer_id").cast("integer"),
#                 col("amount").cast("decimal(10,2)"),
#                 col("order_date").cast("date"),
#                 col("status"),
#                 col("ingestion_time")
#             )
#     )

# ======================================================================
# DATA QUALITY EXPECTATIONS - Examples
# ======================================================================

# Example: Multiple expectation types on one table
@dlt.table(name="validated_customers")
@dlt.expect("has_info", "info IS NOT NULL")                      # WARN: log violations
@dlt.expect_or_drop("positive_amount", "amount > 0")              # DROP: remove invalid
@dlt.expect_or_drop("valid_customer_id", "id IS NOT NULL")  # DROP: remove invalid rows
def validated_customers():
    return dlt.read_stream("bronze_customers")

# Expectation Actions:
# - expect:          WARN - log violations, keep all data
# - expect_or_drop:  DROP - remove invalid records
# - expect_or_fail:  FAIL - stop pipeline if violated

# ======================================================================
# COMPLETE EXAMPLE: Transaction Processing Pipeline
# (Commented out to avoid schema conflicts - uncomment and do full refresh if needed)
# ======================================================================

# # Bronze: Raw transaction ingestion
# @dlt.table(name="bronze_transactions")
# def bronze_transactions():
#     return (
#         dlt.read_stream("bronze_customers")
#             .select(
#                 col("id").alias("customer_id"),
#                 col("transaction_id"),
#                 col("amount"),
#                 col("date").alias("transaction_date"),
#                 col("store_location")
#             )
#     )
# 
# # Silver: Cleaned transactions
# @dlt.table(name="silver_transactions")
# @dlt.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
# @dlt.expect_or_drop("positive_amount", "amount > 0")
# def silver_transactions():
#     return (
#         dlt.read_stream("bronze_transactions")
#             .select(
#                 col("transaction_id"),
#                 col("customer_id"),
#                 col("amount"),
#                 col("transaction_date"),
#                 col("store_location")
#             )
#     )
# 
# # Gold: Revenue by store location
# @dlt.table(name="gold_store_revenue")
# def gold_store_revenue():
#     return (
#         dlt.read("silver_transactions")
#             .groupBy("store_location")
#             .agg(
#                 sum("amount").alias("total_revenue"),
#                 count("transaction_id").alias("transaction_count"),
#                 avg("amount").alias("avg_transaction_value")
#             )
#     )
