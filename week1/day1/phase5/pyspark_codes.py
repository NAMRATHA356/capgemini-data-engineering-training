# Phase 5 – Databricks + Olist End-to-End Data Engineering Pipeline

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, when, to_date, row_number, dense_rank
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("OlistPipeline").getOrCreate()

# Load Data
orders = spark.read.csv('/Volumes/workspace/default/phase5/FileStore/olist/olist_orders_dataset.csv', header=True, inferSchema=True)
customers = spark.read.csv('/Volumes/workspace/default/phase5/FileStore/olist/olist_customers_dataset.csv', header=True, inferSchema=True)
order_items = spark.read.csv('/Volumes/workspace/default/phase5/FileStore/olist/olist_order_items_dataset.csv', header=True, inferSchema=True)
products = spark.read.csv('/Volumes/workspace/default/phase5/FileStore/olist/olist_products_dataset.csv', header=True, inferSchema=True)
payments = spark.read.csv('/Volumes/workspace/default/phase5/FileStore/olist/olist_order_payments_dataset.csv', header=True, inferSchema=True)

# Fix Payment Duplication
payment_agg = payments.groupBy("order_id") \
    .agg(sum("payment_value").alias("order_total"))

# Task 1: Top 3 Customers per City
df1 = orders.join(customers, "customer_id") \
    .join(payment_agg, "order_id")

customer_spend = df1.groupBy("customer_id", "customer_city") \
    .agg(sum("order_total").alias("total_spend"))

windowSpec1 = Window.partitionBy("customer_city") \
    .orderBy(customer_spend["total_spend"].desc())

top_customers = customer_spend.withColumn("rank", row_number().over(windowSpec1)) \
    .filter("rank <= 3")

# Task 2: Running Total of Sales
daily_sales = orders.join(payment_agg, "order_id") \
    .withColumn("date", to_date("order_purchase_timestamp")) \
    .groupBy("date") \
    .agg(sum("order_total").alias("daily_sales"))

windowSpec2 = Window.orderBy("date")

running_total = daily_sales.withColumn(
    "running_total",
    sum("daily_sales").over(windowSpec2)
)

# Task 3: Top Products per Category
df2 = order_items.join(products, "product_id")

prod_sales = df2.groupBy("product_id", "product_category_name") \
    .agg(sum("price").alias("total_sales"))

windowSpec3 = Window.partitionBy("product_category_name") \
    .orderBy(prod_sales["total_sales"].desc())

top_products = prod_sales.withColumn(
    "rank",
    dense_rank().over(windowSpec3)
)

# Task 4: Customer Lifetime Value
clv = df1.groupBy("customer_id") \
    .agg(sum("order_total").alias("total_spend"))

# Task 5: Customer Segmentation
segmented = clv.withColumn(
    "segment",
    when(clv["total_spend"] > 10000, "Gold")
    .when(clv["total_spend"] >= 5000, "Silver")
    .otherwise("Bronze")
)

segment_count = segmented.groupBy("segment").count()

# Task 6: Final Reporting Table
final_report = df1.groupBy("customer_id") \
    .agg(
        sum("order_total").alias("total_spend"),
        count("order_id").alias("total_orders")
    ) \
    .join(customers, "customer_id") \
    .join(segmented.select("customer_id", "segment"), "customer_id")

# Show Outputs
top_customers.show()
running_total.show()
top_products.show()
clv.show()
segmented.show()
segment_count.show()
final_report.show()