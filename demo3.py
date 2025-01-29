# from pyspark.sql import SparkSession
# import pandas as pd
# import re
# from nltk.tokenize import word_tokenize
# from nltk.corpus import stopwords
# import nltk

# # Download NLTK resources
# nltk.download('punkt')
# nltk.download('stopwords')

# def clean_text(file_path):
#     """Clean and tokenize text data."""
#     if pd.isna(file_path):
#         return ""
#     file_path = str(file_path)  # Convert to string
#     file_path = re.sub(r'[^a-zA-Z0-9\s]', '', file_path)
#     file_path = file_path.lower()
#     tokens = word_tokenize(file_path)
#     stop_words = set(stopwords.words('english'))
#     tokens = [word for word in tokens if word not in stop_words]
#     return ' '.join(tokens)

# def process_excel_file(input_file_path, output_file_path):
#     """Process Excel file and save cleaned data."""
#     try:
#         # Load Excel file
#         pandas_excerpts = pd.read_excel(input_file_path)
        
#         # Clean the text in the second column (index 1)
#         pandas_excerpts["cleaned_text"] = pandas_excerpts.iloc[:, 1].apply(clean_text)
        
#         # Save to new Excel file
#         pandas_excerpts.to_excel(output_file_path, index=False)
#         return pandas_excerpts
#     except Exception as e:
#         print(f"Error processing Excel file: {e}")
#         return None

# def create_spark_session():
#     """Create and return a Spark session with specified configuration."""
#     return SparkSession.builder \
#         .config("spark.driver.memory", "4g") \
#         .config("spark.executor.memory", "4g") \
#         .getOrCreate()

# def convert_to_spark(spark, pandas_df):
#     """Convert pandas DataFrame to Spark DataFrame."""
#     try:
#         spark_dataframe = spark.createDataFrame(pandas_df)
#         return spark_dataframe
#     except Exception as e:
#         print(f"Error converting to Spark DataFrame: {e}")
#         return None

# def main():
#     # File paths
#     input_file_path = r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\wikileaks_parsed.xlsx"
#     output_file_path = r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\cleaned_data.xlsx"
    
#     # Create Spark session
#     spark = create_spark_session()
    
#     try:
#         # Process Excel file
#         pandas_df = process_excel_file(input_file_path, output_file_path)
#         if pandas_df is None:
#             raise Exception("Failed to process Excel file")
        
#         # Convert to Spark DataFrame
#         spark_dataframe = convert_to_spark(spark, pandas_df)
        
#         if spark_dataframe is not None:
#             print("Spark DataFrame created successfully:")
#             spark_dataframe.show()
#         else:
#             print("Failed to convert to Spark DataFrame.")
            
#     except Exception as e:
#         print(f"Error in main process: {e}")
#     finally:
#         # Always stop Spark session
#         spark.stop()

# if __name__ == "__main__":
#     main()

from pyspark.sql import SparkSession
import pandas as pd
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk
import sys
import logging

import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\Denxi\\.env\\Scripts\\python-3.10.10-amd64.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\Denxi\\.env\\Scripts\\python-3.10.10-amd64.exe'


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Download NLTK resources
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)

# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.ERROR)

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("TextProcessing") \
            .config("spark.driver.memory", "4g") \
            .config("spark.python.worker.reuse", "false") \
            .config("spark.python.use.daemon", "true") \
            .config("spark.python.worker.memory", "1g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .master("local[*]") \
            .getOrCreate()

        # Set log level to ERROR to reduce noise
        spark.sparkContext.setLogLevel("ERROR")

        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise

def clean_text(text):
    """Clean and tokenize text data."""
    try:
        if pd.isna(text):
            return ""
        text = str(text)  # Convert to string
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
        text = text.lower()
        tokens = word_tokenize(text)
        stop_words = set(stopwords.words('english'))
        tokens = [word for word in tokens if word not in stop_words]
        return ' '.join(tokens)
    except Exception as e:
        logger.error(f"Error in clean_text: {str(e)}")
        return ""

def process_excel_file(input_file_path):
    """Process Excel file and return pandas DataFrame."""
    try:
        logger.info(f"Reading Excel file from: {input_file_path}")
        # Read only the first few rows first to validate
        df_sample = pd.read_excel(input_file_path, nrows=5)
        logger.info(f"Sample columns: {df_sample.columns.tolist()}")
        
        # If sample looks good, read the full file
        pandas_excerpts = pd.read_excel(input_file_path)
        logger.info(f"Successfully read {len(pandas_excerpts)} rows")
        
        # Clean the text in the second column (index 1)
        column_name = pandas_excerpts.columns[1]
        logger.info(f"Cleaning text in column: {column_name}")
        pandas_excerpts["cleaned_text"] = pandas_excerpts[column_name].apply(clean_text)
        
        return pandas_excerpts
    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        raise

def main():
    try:
        # File paths - using raw strings
        input_file_path = r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\wikileaks_parsed.xlsx"  # Changed to Desktop
        output_file_path = r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\cleaned_data.xlsx"     # Changed to Desktop
        
        logger.info("Starting data processing pipeline")
        
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Process Excel file
        pandas_df = process_excel_file(input_file_path)
        logger.info("Excel file processed successfully")
        
        # Save processed data
        pandas_df.to_excel(output_file_path, index=False)
        logger.info(f"Saved processed data to {output_file_path}")
        
        # Convert to Spark DataFrame
        logger.info("Converting to Spark DataFrame")
        spark_dataframe = spark.createDataFrame(pandas_df)
        
        # Show the first few rows
        logger.info("Displaying first few rows of Spark DataFrame:")
        spark_dataframe.show(5, truncate=False)
        
        # Clean up
        spark.stop()
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        if 'spark' in locals():
            spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()