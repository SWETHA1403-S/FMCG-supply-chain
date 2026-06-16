from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("FMCG-Gold-Layer").getOrCreate()

# Read Silver Layer
df = spark.read.option("header", "true").csv(
    "s3://fmcgdatalake/silver-validated-events/"
)

# Convert numeric columns
numeric_cols = [
    "price_unit",
    "delivery_days",
    "stock_available",
    "delivered_qty",
    "units_sold",
    "revenue",
    "inventory_gap"
]

for c in numeric_cols:
    df = df.withColumn(c, col(c).cast("double"))

# =====================================================
# SALES ANALYTICS
# =====================================================

sales_df = df.groupBy(
    "brand",
    "category",
    "region"
).agg(
    sum("revenue").alias("total_revenue"),
    sum("units_sold").alias("total_units_sold")
)

sales_df.write.mode("overwrite") \
    .option("header", "true") \
    .csv("s3://fmcgdatalake/gold-aggregates/sales/")

# =====================================================
# INVENTORY ANALYTICS
# =====================================================

inventory_df = df.groupBy(
    "sku",
    "brand"
).agg(
    avg("stock_available").alias("avg_stock"),
    avg("inventory_gap").alias("avg_inventory_gap"),
    sum("delivered_qty").alias("total_delivered")
)

inventory_df.write.mode("overwrite") \
    .option("header", "true") \
    .csv("s3://fmcgdatalake/gold-aggregates/inventory/")

# =====================================================
# PROMOTION ANALYTICS
# =====================================================

promotion_df = df.groupBy(
    "promotion_flag"
).agg(
    sum("revenue").alias("promo_revenue"),
    sum("units_sold").alias("promo_units_sold"),
    avg("price_unit").alias("avg_price")
)

promotion_df.write.mode("overwrite") \
    .option("header", "true") \
    .csv("s3://fmcgdatalake/gold-aggregates/promotion/")

# =====================================================
# DELIVERY ANALYTICS
# =====================================================

delivery_df = df.groupBy(
    "region",
    "channel"
).agg(
    avg("delivery_days").alias("avg_delivery_days"),
    max("delivery_days").alias("max_delivery_days"),
    min("delivery_days").alias("min_delivery_days")
)

delivery_df.write.mode("overwrite") \
    .option("header", "true") \
    .csv("s3://fmcgdatalake/gold-aggregates/delivery/")

# =====================================================
# ANOMALY DETECTION
# =====================================================

anomalies_df = df.filter(
    (col("delivery_days") > 10) |
    (col("inventory_gap") < -100) |
    (col("stock_available") < 10)
)

anomalies_df.write.mode("overwrite") \
    .option("header", "true") \
    .csv("s3://fmcgdatalake/gold-aggregates/anomalies/")

print("All Gold Aggregates Created Successfully")