Objective

The objective of this project is to build a realistic data pipeline using PySpark for data processing and SQL for analytical queries. The pipeline handles multiple datasets and produces dealer-level and customer-level insights.

Dataset Description

The project uses the following tables:

customers
customer_id
name
city
cars
car_id
brand
model
price
sales
sale_id
customer_id
car_id
sale_date
quantity
dealers
dealer_id
name
city
sales_dealer
sale_id
dealer_id
Project Workflow
Phase 1: Data Understanding
Load all datasets into the Spark environment
Inspect schema and data types
Count records in each dataset
Identify null values, duplicates, and inconsistencies
Phase 2: Data Cleaning
Handle missing values appropriately
Correct invalid values such as negative prices
Trim and standardize string columns
Remove records with invalid foreign keys
Phase 3: Data Validation
Use left_anti join to detect invalid foreign key relationships
Create a validation report summarizing data quality issues
Phase 4: Data Transformation
Calculate total revenue per customer
Perform brand-wise sales aggregation
Generate city-wise revenue insights
Phase 5: Dealer Analytics
Compute revenue per dealer
Identify top-performing dealers
Analyze dealer contribution by city
Phase 6: SQL Analysis
Find top 3 customers per city
Analyze monthly sales trends
Identify repeat customers
Phase 7: Output
Save transformed datasets into tables or files
Document results and insights
Technologies Used
PySpark for data processing
SQL for querying and analysis
Databricks or any Apache Spark environment
Key Outcomes
Identification of high-value customers
Insights into top-performing car brands
Dealer performance evaluation
Understanding customer purchasing patterns
How to Execute
Load all datasets into your Spark environment
Run PySpark scripts for cleaning and transformation
Execute SQL queries for analysis
Store the final outputs for reporting