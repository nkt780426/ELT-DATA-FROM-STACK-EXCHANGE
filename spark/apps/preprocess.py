import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# Function to process HTML and extract text
@udf(returnType=StringType())
def process_html(content):
    cleaned_text = re.sub(r"<.*?>", "", content)
    cleaned_text = re.sub(r"\s+", " ", cleaned_text).strip()
    return cleaned_text

# Function to convert views, num_answer, votes to numeric values
@udf(returnType=StringType())
def convert_to_numeric(value):
    try:
        if value.endswith("k"):
            return str(int(float(value[:-1]) * 1000))
        elif value.endswith("m"):
            return str(int(float(value[:-1]) * 1000000))
        elif value.endswith("b"):
            return str(int(float(value[:-1]) * 1000000000))
        else:
            return str(int(value))
    except ValueError:
        return "0"
