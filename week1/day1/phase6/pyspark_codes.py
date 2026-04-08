from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, sum, count, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Phase6_Pipeline").getOrCreate()

customers = spark.createDataFrame([
    (1, "John ", "john@gmail.com"),
    (2, "Alice", "alice@gmail.com"),
    (3, None, "bob@gmail.com"),
    (4, "Eva", None)
], ["customer_id", "name", "email"])

orders = spark.createDataFrame([
    (101, 1, 1000),
    (102, 2, 2000),
    (103, 99, 500),
    (104, 1, None),
    (105, 2, -100),
    (106, 5, 6000),
    (106, 5, 6000)
], ["order_id", "customer_id", "amount"])

customers_clean = customers \
    .dropna(subset=["customer_id", "name", "email"]) \
    .withColumn("name", trim(col("name")))

orders_clean = orders \
    .filter(col("amount").isNotNull()) \
    .filter(col("amount") >= 0) \
    .dropDuplicates()

print("Clean Customers:")
customers_clean.show()

print("Clean Orders:")
orders_clean.show()

invalid_orders = orders_clean.join(
    customers_clean,
    on="customer_id",
    how="left_anti"
)

print("Invalid Orders:")
invalid_orders.show()

joined_df = orders_clean.join(
    customers_clean,
    on="customer_id",
    how="inner"
)

print("Joined Data:")
joined_df.show()

agg_df = joined_df.groupBy("customer_id", "name") \
    .agg(
        sum("amount").alias("total_spend"),
        count("order_id").alias("order_count")
    )

print("Aggregated Data:")
agg_df.show()

window_spec = Window.orderBy(col("total_spend").desc())

final_df = agg_df.withColumn(
    "rank",
    rank().over(window_spec)
)

print("Final Ranked Data:")
final_df.show()

final_df.write.mode("overwrite").saveAsTable("phase6_output")

print("Data saved as table: phase6_output")