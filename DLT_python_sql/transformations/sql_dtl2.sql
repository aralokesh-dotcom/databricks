/*======================================================================
DLT SQL CODE EXAMPLES
======================================================================

--- BRONZE LAYER ---*/


CREATE OR REFRESH STREAMING TABLE bronze_customers1
COMMENT "Raw customer data from JSON files"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT 
  *,
  current_timestamp() as ingestion_time
FROM cloud_files(
  "/Volumes/main/default/raw_data_volume/exported_users_json/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);


--- SILVER LAYER with EXPECTATIONS (IDEMPOTENT - NO DUPLICATES) ---


CREATE OR REFRESH MATERIALIZED VIEW silver_customers_mv (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT recent_data EXPECT (ingestion_time > current_date() - INTERVAL 30 DAYS)
)
COMMENT "Cleaned customer data with quality checks - deduplicated (NO DUPLICATES)"
TBLPROPERTIES ("quality" = "silver")
AS SELECT DISTINCT
  id,
  name,
  info,
  date,
  active,
  amount,
  ingestion_time
FROM bronze_customers1;


--- GOLD LAYER - Aggregations ---


CREATE OR REFRESH MATERIALIZED VIEW gold_customers_by_active
COMMENT "Customer counts by active status - analytics ready"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  active,
  COUNT(id) as customer_count,
  MIN(date) as first_date,
  MAX(date) as last_date
FROM silver_customers_mv
GROUP BY active;


--- VIEW (Intermediate) ---


CREATE TEMPORARY VIEW active_customers_mv
AS SELECT *
FROM silver_customers_mv
WHERE active = true;
/*=

======================================================================
SQL SYNTAX KEY POINTS
======================================================================

Streaming Table:
  CREATE OR REFRESH STREAMING TABLE table_name ...
  FROM STREAM(source_table)

--Materialized View:
  CREATE OR REFRESH MATERIALIZED VIEW table_name ...
  FROM source_table (no STREAM keyword)

--View (Not Materialized):
  CREATE TEMPORARY VIEW view_name ...

Expectations:
  CONSTRAINT name EXPECT (condition) ON VIOLATION [DROP ROW | FAIL UPDATE]

Auto Loader:
  FROM cloud_files(path, format, options)

IDEMPOTENCY:
  - Materialized Views REPLACE data (no duplicates)
  - Streaming Tables APPEND data (can cause duplicates)
  - Use MV for silver/gold layers to ensure idempotency


======================================================================
END OF DLT CODE EXAMPLES
======================================================================*/