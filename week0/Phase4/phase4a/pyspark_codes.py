from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when
from pyspark.ml.feature import Bucketizer
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

# -------------------------------
# 1. Create Spark Session
# -------------------------------
spark = SparkSession.builder.appName("SalesPipeline").getOrCreate()

# -------------------------------
# 2. Sample Orders Data
# -------------------------------
orders_data = [
    (1, "2026-03-01", 1000),
    (2, "2026-03-01", 500),
    (1, "2026-03-02", 2000),
    (3, "2026-03-02", 300),
    (2, "2026-03-03", 700),
    (4, "2026-03-03", 12000),
    (3, "2026-03-03", 200),
]
orders_columns = ["customer_id", "order_date", "amount"]
orders = spark.createDataFrame(orders_data, orders_columns)

# -------------------------------
# 3. Sample Customers Data
# -------------------------------
customers_data = [
    (1, "Alice", "New York"),
    (2, "Bob", "Los Angeles"),
    (3, "Charlie", "Chicago"),
    (4, "David", "Houston")
]
customers_columns = ["customer_id", "customer_name", "city"]
customers = spark.createDataFrame(customers_data, customers_columns)

# -------------------------------
# 4. Data Cleaning
# -------------------------------
orders = orders.dropna(subset=["customer_id", "amount"])  # remove nulls
orders = orders.dropDuplicates()                         # remove duplicates
orders = orders.filter(col("amount") > 0)               # remove invalid amounts

# -------------------------------
# 5. Join Orders and Customers
# -------------------------------
df = orders.join(customers, on="customer_id", how="inner")

# -------------------------------
# 6. Task 1: Daily Sales
# -------------------------------
daily_sales = df.groupBy("order_date").agg(sum("amount").alias("total_sales"))

# -------------------------------
# 7. Task 2: City-wise Revenue
# -------------------------------
city_revenue = df.groupBy("city").agg(sum("amount").alias("total_revenue"))

# -------------------------------
# 8. Task 3: Top 5 Customers
# -------------------------------
top_customers = df.groupBy("customer_name") \
    .agg(sum("amount").alias("total_spend")) \
    .orderBy(col("total_spend").desc()) \
    .limit(5)

# -------------------------------
# 9. Task 4: Repeat Customers (>1 order)
# -------------------------------
repeat_customers = df.groupBy("customer_id") \
    .agg(count("order_date").alias("order_count")) \
    .filter(col("order_count") > 1)

# -------------------------------
# 10. Task 5: Customer Segmentation (Conditional Logic)
# -------------------------------
customer_spend = df.groupBy("customer_id", "customer_name") \
    .agg(sum("amount").alias("total_spend"))

# Rename to avoid ambiguity
customer_spend_renamed = customer_spend.withColumnRenamed("customer_name", "cust_name")

segmented = customer_spend_renamed.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)

# -------------------------------
# 11. Task 6: Final Reporting Table
# -------------------------------
order_counts = df.groupBy("customer_id").agg(count("order_date").alias("order_count"))

final_df = customers.join(customer_spend_renamed, "customer_id", "left") \
    .join(order_counts, "customer_id", "left") \
    .withColumn(
        "segment",
        when(col("total_spend") > 10000, "Gold")
        .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
        .otherwise("Bronze")
    ) \
    .select(
        col("cust_name").alias("customer_name"),
        col("city"),
        col("total_spend"),
        col("order_count"),
        col("segment")
    )

# -------------------------------
# 12. Optional: Quantile-based Segmentation
# -------------------------------
quantiles = final_df.approxQuantile("total_spend", [0.33, 0.66], 0)
print("Quantiles for total_spend:", quantiles)

# -------------------------------
# 13. Optional: Window-based Ranking
# -------------------------------
window = Window.orderBy("total_spend")
final_df = final_df.withColumn("rank_pct", percent_rank().over(window))

# -------------------------------
# 14. Task 7: Save Output Safely
# -------------------------------
# Save to /tmp/output_report to avoid permission issues
final_df.write.mode("overwrite").option("header", True).csv("/tmp/output_report")

# -------------------------------
# 15. Show Results
# -------------------------------
print("Daily Sales")
daily_sales.show()

print("City Revenue")
city_revenue.show()

print("Top Customers")
top_customers.show()

print("Repeat Customers")
repeat_customers.show()

print("Customer Segmentation")
segmented.show()

print("Final Report")
final_df.show()