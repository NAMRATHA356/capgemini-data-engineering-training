from pyspark.sql import sparkSession
from pyspark.sql functions import col,sum,count

spark = SparkSession.builder.appName("Phase3).getOrCreate()


#Read csv files                                 
customers = spark.read.format("csv").option("header","true").load("/samples/customers.csv")
sales = spark.read.format("csv").option("header","true").load("/samples/sales.csv")

#1. show the data
customers.show()
sales.show()

#2. print the schema
customers.printSchema()
sales.printSchema()

#3.Identify missing values(here in sample data there is no null value so the output is false for all the columns)
for c in customers.columns:
    customers.select(col(c).isNull().alias(c)).show()

#4. Remove null values
customers_cleaned = customers.dropna()
sales_cleaned = sales.dropna()

#5. Calculate Daily sales
daily_sales = sales_cleaned.groupBy("sale_date").agg(sum("total_amount").alias("daily_total"))
daily_sales.show()

#6. city-wise revenue
customer_sales = sales_cleaned.join(customers_cleaned, "customer_id")
city_revenue = customer_sales.groupBy("city").agg(sum("total_amount").alias("total_revenue"))
city_revenue.show()

#7. repeat customers (>=2orders) since there is no customers with greater than 2 orders so i have executed with >=2 order
repeat_customers = sales_cleaned.groupBy("customer_id").agg(count("*").alias("order_count")).filter(col("order_count") >= 2)
repeat_customers.show()

#8. Find highest spending customer in each city
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

customer_spend = customer_sales.groupBy("customer_id", "city").agg(sum("total_amount").alias("total_spend"))
window_spec = Window.partitionBy("city").orderBy(col("total_spend").desc())
top_customers = customer_spend.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)
top_customers.show()

#9. Build final reporting table with customer, city, total spend, order count
final_report = customer_sales.groupBy("customer_id", "city").agg(sum("total_amount").alias("total_spend"),count("*").alias("order_count"))
final_report.show()

final_report.write.format("csv").option("header", "true").save("/outputs/final_report")