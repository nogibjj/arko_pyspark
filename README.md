[![CICD](https://github.com/nogibjj/arko_pyspark/actions/workflows/CICD.yml/badge.svg)](https://github.com/nogibjj/arko_pyspark/actions/workflows/CICD.yml)

# AAPL Stock Data Processing using PySpark

This project processes AAPL stock data using PySpark, performs various transformations on the data, executes complex Spark SQL queries, and outputs the results in CSV format. Additionally, it includes unit tests to verify the correctness of the transformations and queries.

## Project Overview

The main objective of this project is to:
1. **Transform** raw AAPL stock data by adding useful columns like:
   - Previous close price
   - Percent change in closing price
   - Trend (Up, Down, No Change)
   - 30-day moving average of closing price
   - High-low range for each day
2. **Execute** complex Spark SQL queries to filter and analyze the data.
3. **Generate Output**:
   - A filtered dataset based on the applied transformations and queries.
   - A summary report about the data processing.
4. **Test**: Includes unit tests to ensure correctness of the processing steps.

## File Structure

.
├── LICENSE
├── Makefile
├── README.md
├── data
│   └── AAPL.csv
├── requirements.txt
├── src
│   ├── __init__.py
│   └── pyspark_transform.py
└── tests
    ├── __pycache__
    │   └── test_pyspark.cpython-38.pyc
    └── test_pyspark.py


## Project Setup
### 1. Clone the Repository

Clone the repository to your local machine:

```bash
git clone https://github.com/nogibjj/arko_pyspark
cd arko_pyspark
```

### 2. Run the script

```bash
make install format lint test
.venv/bin/python src/pyspark_transform.py
```

or (for dockerized version)

```bash
make all
```

