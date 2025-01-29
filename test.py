import os
import sys
from pyspark.sql import SparkSession
import pandas as pd
import findspark
findspark.init()
# Print environment info
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
print(f"Working directory: {os.getcwd()}")

def create_spark_session():
    """Create and configure Spark session."""
    os.environ['PYSPARK_PYTHON'] = sys.executable  # Use the current Python interpreter
    
    return SparkSession.builder \
        .appName("TextProcessing") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.python.worker.memory", "1g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.local.dir", os.path.join(os.path.expanduser("~"), "temp_spark")) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[2]") \
        .getOrCreate()

# Create temp directory if it doesn't exist
temp_dir = os.path.join(os.path.expanduser("~"), "temp_spark")
os.makedirs(temp_dir, exist_ok=True)

# Create Spark session
spark = create_spark_session()

try:
    # Create a simple test DataFrame
    test_df = pd.DataFrame({'col1': range(5), 'col2': range(5)})
    spark_df = spark.createDataFrame(test_df)

    # Try to perform a simple operation
    print("Testing Spark DataFrame:")
    spark_df.show()

except Exception as e:
    print(f"Error occurred: {str(e)}")
    
finally:
    # Clean up
    spark.stop()