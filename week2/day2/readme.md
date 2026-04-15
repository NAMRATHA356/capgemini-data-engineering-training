Delta Lake Assignment - README
Project Overview

This project demonstrates core concepts of Delta Lake using PySpark, including table creation, data manipulation, schema evolution, and time travel. The dataset represents simple customer purchase transactions.

Reference dataset and tasks:

Technologies Used
PySpark
Delta Lake
Apache Spark
Dataset Description

The dataset contains the following columns:

id - Unique transaction ID
customer_id - Customer identifier
product - Product name
amount - Transaction amount
Tasks Performed
1. Create Delta Table
Convert the initial DataFrame into a Delta table.
Store data in Delta format.
2. Insert New Data
Add a new record:
(5, "C005", "Camera", 30000)
3. Update Existing Data
Update the amount where:
id = 2 → amount = 18000
4. Delete Data
Delete the record where:
id = 1
5. MERGE Operation (Upsert)
Perform incremental load using MERGE:
Update existing record:
id = 3 → amount = 22000
Insert new record:
id = 6 → Watch
6. Schema Evolution
Add a new column:
country
7. Time Travel
Access previous versions of the table.
Restore table to an older version if needed.