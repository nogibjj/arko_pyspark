import unittest
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, when
from pyspark.sql.window import Window


class TestAAPLStockDataProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for tests
        cls.spark = (
            SparkSession.builder.appName("AAPL Stock Data Processing Test")
            .master("local[*]")
            .getOrCreate()
        )

        # Sample data to mimic "data/AAPL.csv"
        data = [
            ("2024-01-01", 150.0, 155.0, 1000, 160.0, 149.0),
            ("2024-01-02", 155.0, 158.0, 2000000, 161.0, 154.0),
            ("2024-01-03", 158.0, 157.0, 3000000, 162.0, 156.0),
        ]
        columns = ["Date", "Open", "Close", "Volume", "High", "Low"]
        cls.df = cls.spark.createDataFrame(data, columns)

        # Temporary output path
        cls.output_path = "output/test_data"
        cls.summary_report = os.path.join(cls.output_path, "summary_report.md")

        # Run the transformation as per the main script
        cls.df.createOrReplaceTempView("AAPL")

        window_spec = Window.orderBy("Date")
        cls.df = (
            cls.df.withColumn("prev_close", lag("Close", 1).over(window_spec))
            .withColumn(
                "percent_change",
                ((col("Close") - col("prev_close")) / col("prev_close")) * 100,
            )
            .withColumn(
                "trend",
                when(col("Close") > col("prev_close"), "Up")
                .when(col("Close") < col("prev_close"), "Down")
                .otherwise("No Change"),
            )
            .withColumn(
                "30_day_avg_close", avg("Close").over(window_spec.rowsBetween(-29, 0))
            )
            .withColumn("high_low_range", col("High") - col("Low"))
        )

        cls.df.createOrReplaceTempView("AAPL")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_column_addition(self):
        """Test if the transformation columns are added."""
        self.assertIn("prev_close", self.df.columns)
        self.assertIn("percent_change", self.df.columns)
        self.assertIn("trend", self.df.columns)
        self.assertIn("30_day_avg_close", self.df.columns)
        self.assertIn("high_low_range", self.df.columns)

    def test_sql_query(self):
        """Test the SQL query for correct filtering and columns."""
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
              AND Volume > 1000000
        ORDER BY Date
        """
        result_df = self.spark.sql(query)

        # Check that we have the expected number of records
        # In this case, only records with Volume > 1000000 should be selected
        expected_count = self.df.filter("Volume > 1000000").count()
        self.assertEqual(result_df.count(), expected_count)

        # Validate column names
        expected_columns = [
            "Date",
            "Open",
            "Close",
            "Volume",
            "percent_change",
            "trend",
            "30_day_avg_close",
            "high_low_range",
        ]
        self.assertListEqual(result_df.columns, expected_columns)

    def test_output_file_creation(self):
        """Test if output files are generated."""
        # Run SQL query and write to CSV
        result_df = self.spark.sql(
            """
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
                  AND Volume > 1000000
            ORDER BY Date
        """
        )

        # Create output directory if it doesn't exist
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

        # Write filtered data to CSV
        result_df.write.mode("overwrite").csv(
            os.path.join(self.output_path, "AAPL_complex_filtered.csv"), header=True
        )

        # Write summary report
        summary = f"Total records: {self.df.count()}\nFiltered records (high volume & non-null fields): {result_df.count()}"
        with open(self.summary_report, "w") as f:
            f.write(summary)

        # Verify that files are created
        self.assertTrue(
            os.path.exists(os.path.join(self.output_path, "AAPL_complex_filtered.csv"))
        )
        self.assertTrue(os.path.exists(self.summary_report))


# Run the tests
if __name__ == "__main__":
    unittest.main()
