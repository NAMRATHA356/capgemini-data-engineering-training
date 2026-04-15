Advanced Data Engineering Project – Insurance Domain
Overview

This project focuses on building an end-to-end data pipeline in the insurance domain. The goal is to process and analyze data related to customers, policies, claims, and agents to derive meaningful business insights such as premium collection, claim trends, risk analysis, and agent performance.

Data Description

The dataset consists of the following tables:

customers: Contains customer details
policies: Policies purchased by customers
claims: Claims raised against policies
agents: Agent information
policy_agent: Mapping between policies and agents
Business Flow

Customer → Policy → Premium → Claims → Agent

Phase 1: Data Understanding
Loaded all datasets using PySpark
Checked schema using printSchema()
Verified row counts for each table
Identified issues such as null values, invalid keys, and inconsistencies
Understood relationships between tables
Phase 2: Data Cleaning
Handled null values using appropriate strategies (fill or drop)
Removed or corrected negative values in premium and claim_amount
Standardized string columns (trim, case formatting)
Converted columns to correct data types (date, numeric)
Phase 3: Data Validation
Used joins to validate foreign key relationships
Identified invalid records using left anti joins
Created validation metrics:
Total records
Invalid records
Cleaned records
Ensured consistency before and after transformations
Phase 4: Data Transformation
Joined datasets carefully to avoid duplication
Calculated:
Total premium per customer
Total claim per customer
Risk score = total_claim / total_premium
Generated city-wise distribution of premium and claims
Phase 5: Advanced SQL (CTE)
Created temporary views from DataFrames
Used Common Table Expressions (CTE) to simplify complex queries
Broke queries into logical steps:
Customer premium aggregation
Customer claim aggregation
Risk score calculation
Identified:
Top risky customers per city
Customers with increasing claim trends
Phase 6: Window Functions
Used ROW_NUMBER, RANK, and DENSE_RANK
Ranked:
Agents based on premium handled
Customers based on risk score
Applied correct partitioning logic
Phase 7: Final Output
Generated final analytical datasets
Stored outputs in tables
Ensured all transformations were validated