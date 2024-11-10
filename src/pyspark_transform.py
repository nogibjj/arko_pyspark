from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("AAPL Stock Data Processing").getOrCreate()

# Load Data
data_path = "data/AAPL.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Define a window specification for calculating lag and moving average
window_spec = Window.orderBy("Date")

# Data Transformation: Adding necessary columns
df = (
    df.withColumn("prev_close", lag("Close", 1).over(window_spec))
    .withColumn(
        "percent_change", ((col("Close") - col("prev_close")) / col("prev_close")) * 100
    )
    .withColumn(
        "trend",
        when(col("Close") > col("prev_close"), "Up")
        .when(col("Close") < col("prev_close"), "Down")
        .otherwise("No Change"),
    )
    .withColumn("30_day_avg_close", avg("Close").over(window_spec.rowsBetween(-29, 0)))
    .withColumn("high_low_range", col("High") - col("Low"))
)

# Create a temporary view for Spark SQL queries
df.createOrReplaceTempView("AAPL")

# Complex Spark SQL Query
query = """
SELECT Date,
       Open,
       Close,
       Volume,
       percent_change,
       trend,
       30_day_avg_close,
       high_low_range
FROM AAPL
WHERE percent_change IS NOT NULL
      AND 30_day_avg_close IS NOT NULL
      AND Volume > 1000000  -- Filter for days with high trading volume
ORDER BY Date
"""

# Execute the query
filtered_df = spark.sql(query)

# Write output
filtered_df.write.mode("overwrite").csv(
    "output/processed_data/AAPL_complex_filtered.csv", header=True
)

# Summary report
summary = f"Total records: {df.count()}\nFiltered records (high volume & non-null fields): {filtered_df.count()}"
with open("output/summary_report.md", "w") as f:
    f.write(summary)

# Stop Spark session
spark.stop()
