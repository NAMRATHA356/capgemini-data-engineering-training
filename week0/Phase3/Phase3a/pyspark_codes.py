from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]
customers = spark.createDataFrame(data, columns)
print("Data Before Cleaning:")
customers.show()

# Check initial count
initial_count = customers.count()
print("Row Count Before:", initial_count)

# Remove rows where customer_id is null
customers_clean = customers.filter(col("customer_id").isNotNull())

# Handle missing name with replace with "Unknown"
customers_clean = customers_clean.fillna({"name": "Unknown"})

# Handle missing city with replace with "Unknown"
customers_clean = customers_clean.fillna({"city": "Unknown"})

# Remove duplicate rows
customers_clean = customers_clean.dropDuplicates()

# Remove invalid age (age should be > 0)
customers_clean = customers_clean.filter(col("age") > 0)

print("Cleaned Data:")
customers_clean.show()

# Validate count after cleaning
final_count = customers_clean.count()
print("Row Count After:", final_count)

# Count number of customers per city
customers_per_city = customers_clean.groupBy("city").agg(count("customer_id").alias("total_customers"))
customers_per_city.show()