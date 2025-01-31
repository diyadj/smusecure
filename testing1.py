from pyspark.sql import SparkSession 
import pyspark.pandas as ps 
import pandas as pd 
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk

# Download NLTK resources
nltk.download('punkt')
nltk.download('stopwords')

def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text)  # Convert to string
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    text = text.lower()
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words('english'))
    tokens = [word for word in tokens if word not in stop_words]
    return ' '.join(tokens)

try:
    # Load and process Excel file
    file_path = "./wikileaks_parsed.xlsx"
    df = pd.read_excel(file_path)
    df["cleaned_text"] = df.iloc[:, 1].apply(clean_text)
    output_file_path = "./cleaned_data.xlsx"
    df.to_excel(output_file_path, index=False)

    def convertSpark(file_path=output_file_path):
        # Create Spark session with configuration
        spark = SparkSession.builder\
            .config("spark.driver.memory", "4g")\
            .config("spark.executor.memory", "4g")\
            .getOrCreate()
        
        try:
            pf = pd.read_excel(file_path)
            df = spark.createDataFrame(pf)
            return df
        except Exception as e:
            print(f"Error converting to Spark DataFrame: {e}")
            return None
        finally:
            spark.stop()

except Exception as e:
    print(f"Error processing file: {e}")

# For pandas DataFrame
print("Pandas DataFrame:")
print(df.head())  # Shows first 5 rows
print(df.info())  # Shows column info and data types

# For Spark DataFrame
spark_df = convertSpark()
print("\nSpark DataFrame:")
spark_df.show()  # Shows first 20 rows
spark_df.printSchema()  # Shows schema