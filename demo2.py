from pyspark.sql import SparkSession 
import pandas as pd 
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import nltk

# Download NLTK resources
nltk.download('punkt')
nltk.download('stopwords')

output_file_path = r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\cleaned_data.xlsx"

def clean_text(file_path):
    if pd.isna(file_path):
        return ""
    file_path = str(file_path)  # Convert to string
    file_path = re.sub(r'[^a-zA-Z0-9\s]', '', file_path)
    file_path = file_path.lower()
    tokens = word_tokenize(file_path)
    stop_words = set(stopwords.words('english'))
    tokens = [word for word in tokens if word not in stop_words]
    return ' '.join(tokens)


    # Load and process Excel file
    file_path = r"C:\Users\Denxi\OneDrive - Singapore Management University\Documents\GitHub\smusecure\wikileaks_parsed.xlsx"
    pandas_excerpts = pd.read_excel(file_path)  # df is pandas dataframe
    pandas_excerpts["cleaned_text"] = pandas_excerpts.iloc[:, 1].apply(clean_text)  # creates new column in dataframe
    
    pandas_excerpts.to_excel(output_file_path, index=False)

    def convertSpark(file_path):  # converts excerpts to spark dataframe
        # Create Spark session with configuration
        spark = SparkSession.builder \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        
        try:
            pandas_excerpts_2 = pd.read_excel(file_path)  # pf is pandas dataframe
            spark_dataframe = spark.createDataFrame(pandas_excerpts_2)  # turns pf into Spark dataframe
            #spark.stop()  # stop the spark session
            return spark_dataframe
        except Exception as e:
            print(f"Error converting to Spark DataFrame: {e}")
            return None



# Call the function to convert to Spark DataFrame
spark_dataframe = convertSpark(file_path)

# Check if the conversion was successful
if spark_dataframe:
    print(spark_dataframe)  # Print the Spark DataFrame object
    spark_dataframe.show()  # Show the first few rows of the Spark DataFrame
    spark.stop()
else:
    print("Failed to convert to Spark DataFrame.")
