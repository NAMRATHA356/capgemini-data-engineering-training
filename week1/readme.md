Overview

This assignment focuses on learning and applying Regular Expressions (REGEX) in SQL using Spark / Databricks.

The goal is to extract meaningful information from structured and semi-structured data such as:

Emails
Phone numbers
Employee codes
Mixed alphanumeric values
File Reference
Assignment Questions and Data:
Table Details
Table Name: regex_practice
Column Name	Description
id	Unique identifier
full_text	Structured text containing codes
email	Email addresses
phone	Phone numbers with country codes
mixed_value	Mixed alphanumeric strings
Technologies Used
SQL (Spark SQL / Databricks)
REGEX functions:
regexp_extract()
regexp_replace()
Key Concepts
1. Pattern Matching

Using REGEX to identify specific patterns in text.

Examples:

Extract numbers → [0-9]+
Extract alphabets → [A-Za-z]+
2. Position-Based Extraction
Start of string → ^
End of string → $

Examples:

^[0-9]+ → numbers at beginning
[0-9]+$ → numbers at end
3. Group Extraction

Parentheses () are used to extract specific parts of a match.

Example:

regexp_extract(email, '@(.+)', 1)
4. Email Parsing
Username → before @
Domain → after @
Extension → after last .
5. Phone Number Processing
Extract country code
Handle formats with and without +
6. Structured Text Parsing
Extract values between underscores
Extract employee IDs
Extract country codes
Assignment Structure
Section 1: Mixed Value Processing
Extract digits from start and end
Extract fixed-length numbers
Extract single characters
Section 2: Email Analysis
Extract username
Extract domain
Extract extension
Section 3: Phone Number Analysis
Extract country codes
Handle multiple formats
Section 4: Full Text Parsing
Extract employee numbers
Extract substrings between delimiters
Extract fixed positions
Key Learning Outcomes

After completing this assignment, you will be able to:

Use REGEX in Spark SQL effectively
Extract structured data from text fields
Handle real-world messy datasets
Apply pattern matching for data cleaning
Understand grouping and pattern logic
How to Run
Create the table using the provided schema
Insert all records
Write queries using regexp_extract()
Validate outputs carefully
Important Note

Spark SQL does not support Oracle-style REGEXP_SUBSTR with multiple arguments.

Correct usage:

regexp_extract(column, pattern, group)
Conclusion

This assignment builds strong skills in:

Data cleaning
Pattern extraction
Text processing

These skills are essential for:

Data Analysts
Data Engineers
SQL Developers