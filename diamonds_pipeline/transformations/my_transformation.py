import dlt
from pyspark.sql.functions import col, current_timestamp, expr, when, sum, count

# ── BRONZE ────────────────────────────────────────────────
@dlt.table(
    name="orders_bronze",
    comment="Raw orders from Volume landing zone"
)
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            # ✅ Use Volume path — NOT /data/ or dbfs:/
            .option("cloudFiles.schemaLocation",
                    "/Volumes/my_catalog/orders_db/landing_data/schema_hints/orders")
            .load("/Volumes/my_catalog/orders_db/landing_data/orders/")
            .select(
                col("order_id"),
                col("customer_id"),
                col("amount").cast("double"),
                col("status"),
                col("order_date"),
                current_timestamp().alias("ingested_at")
            )
    )

@dlt.table(
    name="orders_silver",
    comment="Cleaned and validated orders",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_order_id",    "order_id IS NOT NULL")          # warn only
@dlt.expect_or_drop("valid_amount",    "amount > 0")              # drop bad rows
@dlt.expect_or_fail("no_future_dates", "order_date <= current_date()") # halt pipeline
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")     # reads from Bronze (streaming)
            .select(
                col("order_id"),
                col("customer_id"),
                col("amount"),
                col("status"),
                col("order_date"),
                col("ingested_at")
            )
    )
@dlt.table(name="orders_gold")
def orders_gold():
    return (
        dlt.read("orders_silver")
            .groupBy("status")
            .agg(
                sum("amount").alias("total_amount"),
                count("order_id").alias("order_count")
            )
    )
@dlt.view(name="orders_enriched_view")
def orders_enriched_view():
    return dlt.read("orders_silver").filter(col("status") != "CANCELLED")


# Gold: final aggregated materialized table
@dlt.table(
    name="orders_gold_by_segment",
    comment="Daily order totals by customer segment"
)
def orders_gold_by_segment():
    return (
        dlt.read("orders_enriched_view")
            .groupBy("customer_id", "order_date")
            .agg(
                sum("amount").alias("total_revenue"),
                count("order_id").alias("order_count")
            )
    )
