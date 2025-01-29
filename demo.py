from pyspark.sql import SparkSession
import pyspark.pandas as ps
import pandas as pd
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk
import sys
import logging
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Download NLTK resources
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)

# Create Spark session
spark = SparkSession.builder \
    .appName("ExcelToSpark") \
    .getOrCreate()

#clean original TEXT; returns text string 
# Load the CSV file
df = spark.read.csv(r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\news_excerpts_parsed - Sheet1.csv", header=True, inferSchema=True)

# Define the text cleaning function
def clean_text(text):
    if text is None:
        return ""   
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    text = text.lower()
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words('english'))
    tokens = [word for word in tokens if word not in stop_words]
    return ' '.join(tokens)

# Register the UDF with Spark
clean_text_udf = udf(clean_text, StringType())

# Apply the cleaning function to the 'text_column' (replace 'text_column' with your actual column name)
df = df.withColumn("cleaned_text", clean_text_udf(df["Text"]))

# # Save the cleaned data to a single CSV file
df.write.csv(r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\cleaned_data.csv")

#turn cleaned csv into excel
# # Specify the file paths
# csv_file_path = r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\cleaned_data.csv"  # Replace with your CSV file path
# excel_file_path = r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\cleaned_data_excel.xlsx"  # Desired output Excel file path

# # # Read the CSV file
# # data = pd.read_csv(csv_file_path)

# # # Write to Excel
# # data.to_excel(excel_file_path, index=False, engine="openpyxl")  # Use openpyxl engine for .xlsx
# # print(f"CSV file has been converted to Excel at: {excel_file_path}")


# # # Read Excel file after tokenization
# # pf = pd.read_excel(excel_file_path)

# # Convert to Spark DataFrame
# df_cleaned = spark.createDataFrame(df_cleaned)

# # Show the DataFrame
# # print(df_cleaned)
# df_cleaned.show()