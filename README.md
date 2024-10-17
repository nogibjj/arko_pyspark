[![Install](https://github.com/nogibjj/arko_complex_query/actions/workflows/install.yml/badge.svg)](https://github.com/nogibjj/arko_complex_query/actions/workflows/install.yml)
[![Format](https://github.com/nogibjj/arko_complex_query/actions/workflows/format.yml/badge.svg)](https://github.com/nogibjj/arko_complex_query/actions/workflows/format.yml)
[![Lint](https://github.com/nogibjj/arko_complex_query/actions/workflows/lint.yml/badge.svg)](https://github.com/nogibjj/arko_complex_query/actions/workflows/lint.yml)
[![Test](https://github.com/nogibjj/arko_complex_query/actions/workflows/test.yml/badge.svg)](https://github.com/nogibjj/arko_complex_query/actions/workflows/test.yml)

# CLI Tool to interact with DataBricks table 

This project is to demonstrate how to perform ETL processes on a dataset and creating a CLI tool enable users to interact with the extranal databse and perfomr complex queries on it.

## Project Function
- A `main.py` script which serves as the CLI structure and logic.
- A `test_main.py` script to unit test the different functionalities.
- A `mylib/extract.py` script to extract a csv file from github.
- A `mylib/transform_load.py` script to remove all unwanted columns and then load it to a sqlite database
- A `mylib/query.py` script to perform CRUD queries on the dataset along with the option to execute custom queries.
  
# Databricks AAPL Table Operations

This document provides detailed explanations of each function in the Databricks AAPL Table Operations script. The script performs basic CRUD (Create, Read, Update, Delete) operations on the AAPL table using the Databricks SQL API.

## Functions Overview

### 1. `create(entry)`

Inserts a new entry into the AAPL table.

**Parameters:**
- `entry`: A tuple containing the values to insert into the table. The tuple must match the number of columns in the AAPL table, which are typically Date, Open, High, Low, Close, and Volume.

**Returns:**
- A string message indicating that the entry has been added successfully.

**Example:**
```python
new_entry = ('2024-01-01', 150.0, 155.0, 149.0, 154.0, 1000000)
create(new_entry)
```

### 2. `read()`

Reads and displays the first 5 rows from the AAPL table.

**Returns:**
- A string message indicating success after displaying the query results in a tabular format.

**Example:**
```python
read()
```

### 3. `custom_query(query)`

Runs a custom SQL query on the AAPL table.

**Parameters:**
- `query`: A string containing the SQL query you wish to execute. This allows for flexible querying based on specific requirements.

**Returns:**
- A string message indicating success after displaying the results of the custom query.

**Example:**
```python
custom_sql_query = "SELECT * FROM AAPL WHERE Close > 150"
custom_query(custom_sql_query)
```

### 4. `update(column, new_value, condition_column, condition_value)`

Updates a specific value in the AAPL table.

**Parameters:**
- `column`: The column to update (e.g., 'Close').
- `new_value`: The new value to set in the specified column.
- `condition_column`: The column to check in the WHERE condition (e.g., 'Date').
- `condition_value`: The value in the condition_column to match for updating (e.g., '2024-01-01').

**Returns:**
- A string message indicating that the entry has been updated successfully.

**Example:**
```python
update("Close", 155.0, "Date", "2024-01-01")
```

### 5. `delete(condition_column, condition_value)`

Deletes a row from the AAPL table.

**Parameters:**
- `condition_column`: The column to check in the WHERE condition (e.g., 'Date').
- `condition_value`: The value in the condition_column to match for deletion (e.g., '2024-01-01').

**Returns:**
- A string message indicating that the entry has been deleted successfully.

**Example:**
```python
delete("Date", "2024-01-01")
```

### 6. Default Query in `custom_query`

The default query included in the `custom_query` function is designed to analyze price data from the AAPL table. Here’s a breakdown of the query:

```sql
WITH PriceData AS (
    SELECT 
        CAST(Date AS DATE) AS Date,  
        CAST(Open AS FLOAT) AS Open,      
        CAST(High AS FLOAT) AS High,      
        CAST(Low AS FLOAT) AS Low,        
        CAST(Close AS FLOAT) AS Close,    
        CAST(Volume AS BIGINT) AS Volume, 
        (CAST(High AS FLOAT) - CAST(Low AS FLOAT)) AS price_range,  
        SUM(CAST(Volume AS BIGINT)) OVER() AS total_volume,  
        AVG(Close) OVER (ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_close
    FROM 
        aapl
)
SELECT 
    Date, 
    Open, 
    High, 
    Low, 
    Close, 
    Volume, 
    price_range, 
    moving_avg_close, 
    total_volume
FROM 
    PriceData
ORDER BY 
    Date ASC
LIMIT 10;
```

**Explanation:**
- The `WITH` clause creates a Common Table Expression (CTE) named `PriceData` that computes various metrics based on the AAPL table data.
- It selects and casts the Date, Open, High, Low, Close, and Volume columns to appropriate data types.
- It calculates the `price_range` as the difference between High and Low prices.
- It computes the `total_volume` using a window function to get the sum of Volume across all rows.
- It calculates the `moving_avg_close` for the last three closing prices using another window function.
- The final `SELECT` statement retrieves relevant columns from `PriceData`, orders them by Date, and limits the output to the first 10 rows.

## Conclusion

These functions provide a straightforward interface for interacting with the AAPL table in Databricks, allowing users to perform essential data operations efficiently.

![image](https://github.com/user-attachments/assets/fc27b7c0-e55d-45d5-9cd0-b92fcad6eb6e)



## Project Structure

- `mylib/`: Contains the ETL scripts.
- `requirements.txt`: Lists the Python dependencies.
- `Makefile`: Defines common tasks like installing dependencies, running tests, linting, and running docker.
- `.devcontainer/`: Contains `Dockerfile` and VS Code configuration.
- `.github/workflows/`: Contians CI/CD workflows for GitHub.
![image](https://github.com/user-attachments/assets/86b856c0-d55a-486e-8811-984f8011456e)

## Project Setup
### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/nogibjj/arko-sqlite-lab
cd arko-sqlite-lab
```

### 2. Run CLI tool

```bash
.venv/bin/python main.py
```
![image](https://github.com/user-attachments/assets/5bc43730-4143-43e3-b952-6d6bb4e63047)

