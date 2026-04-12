Objective

Build a realistic data pipeline using PySpark for data transformation and SQL for analysis.
The project focuses on handling larger datasets and generating dealer-level analytics.

Dataset Tables

The pipeline uses the following datasets:

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
Pipeline Phases
Phase 1 – Data Understanding
Load all datasets into PySpark
Check schema and record counts
Identify null values, duplicates, and invalid data
Phase 2 – Data Cleaning
Handle missing values
Correct negative prices
Trim unnecessary spaces in strings
Remove invalid foreign key records
Phase 3 – Data Validation
Detect invalid foreign keys using left_anti join
Generate a validation report for data quality checks
Phase 4 – Data Transformation
Customer revenue calculation
Brand-wise sales analysis
City-wise revenue distribution
Phase 5 – Dealer Analytics
Revenue per dealer
Top dealers by revenue
Dealer contribution by city
Phase 6 – SQL Analysis
Top 3 customers per city
Monthly sales trends
Repeat customers analysis
Phase 7 – Output
Save processed datasets (tables/files)
Prepare documentation and insights
Technologies Used
PySpark
SQL
Databricks or any Spark environment