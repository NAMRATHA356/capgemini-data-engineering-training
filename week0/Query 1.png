1. Total Order Amount per Customer
from pyspark.sql.functions import sum

sales.groupBy("customer_id") \
     .agg(sum("total_amount").alias("total_spend")) \
     .show()
2. Top 3 Customers by Total Spend
from pyspark.sql.functions import sum, col

sales.groupBy("customer_id") \
     .agg(sum("total_amount").alias("total_spend")) \
     .orderBy(col("total_spend").desc()) \
     .limit(3) \
     .show()
3. Customers with No Orders
customers.join(
    sales,
    on="customer_id",
    how="left_anti"
).show()
4. City-wise Total Revenue
from pyspark.sql.functions import sum

customers.join(sales, "customer_id") \
         .groupBy("city") \
         .agg(sum("total_amount").alias("total_revenue")) \
         .show()
5. Average Order Amount per Customer
from pyspark.sql.functions import avg

sales.groupBy("customer_id") \
     .agg(avg("total_amount").alias("avg_amount")) \
     .show()
6. Customers with More Than One Order
from pyspark.sql.functions import count, col

sales.groupBy("customer_id") \
     .agg(count("*").alias("order_count")) \
     .filter(col("order_count") > 1) \
     .show()
7. Sort Customers by Total Spend (Descending)
from pyspark.sql.functions import sum

sales.groupBy("customer_id") \
     .agg(sum("total_amount").alias("total_spend")) \
     .orderBy("total_spend", ascending=False) \
     .show()