# Advanced SQL Assignment – Student Submission Analysis (PySpark)

## Objective  
The goal of this assignment is to analyze student submission data using PySpark. The focus is on understanding data inconsistencies, identifying duplicates, and classifying submissions using SQL concepts like joins and window functions.

---

## Datasets  

Three datasets were used in this project:

- **Master Table**  
  Contains 56 unique students with:
  - student_id  
  - college_email  
  - personal_email  

- **Task1_Responses**  
  Contains 51 submission records based on email  

- **Task1_File2**  
  Contains 60 records including duplicates, invalid entries, and extra data  

---

## Tools and Technologies  

- PySpark (DataFrame API and Spark SQL)  
- Databricks  

---

## Phase 1: Data Preparation  

In this phase, the data was cleaned and standardized:

- Converted emails to lowercase using `LOWER()`  
- Removed extra spaces using `TRIM()`  
- Created a unified email mapping table linking both emails to a single `student_id`  
- Used `COALESCE()` to combine multiple email columns  

---

## Phase 2: Core Analysis  

Different joins were used to analyze submissions:

- **Not Submitted**  
  Used `LEFT JOIN` to find students with no matching submissions  

- **Valid Submissions**  
  Used `INNER JOIN` with the master table to ensure submissions belong to valid students  

- **Invalid Submissions**  
  Identified emails not present in the master dataset  

---

## Phase 3: Duplicate Detection  

Window functions were used to detect duplicates:

- Applied `ROW_NUMBER()` with:
  - `PARTITION BY student_id`  
  - `ORDER BY timestamp`  

- Logic:
  - First record → valid submission  
  - Remaining records → duplicates  

**Note:**  
Window functions do not reduce rows, unlike `GROUP BY`.

---

## Phase 4: Advanced Insights  

Performed additional analysis:

- Counted submissions per student  
- Identified students using both email types  
- Created final classification:
  - Submitted  
  - Not Submitted  
  - Duplicate  
  - Invalid  

---

## Key Learnings  

- Data cleaning and normalization  
- Use of joins (INNER, LEFT)  
- Difference between `GROUP BY` and window functions  
- Handling duplicate and invalid data  
- Building classification logic  

---

## Challenges  

- Handling inconsistent email formats  
- Mapping multiple email sources  
- Detecting duplicates without losing data  
- Choosing correct SQL operations  

---

## Future Scope  

- Build a dashboard table for reporting  
- Optimize Spark SQL queries  
- Analyze execution plans  
- Extend solution for multiple tasks  

---

## Conclusion  

This assignment demonstrates how PySpark can be used to process and analyze real-world datasets. It provides a strong understanding of data cleaning, joins, and window functions for handling inconsistencies and extracting insights.