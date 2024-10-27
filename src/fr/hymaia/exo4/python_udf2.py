
from src.fr.hymaia.spark_utils import spark
import pyspark.sql.functions as f
import time

@f.udf('string')
def extract_category_name_udf(category_id):
    return 'food' if int(category_id) < 6 else 'furniture'

def add_category_name(df):
    return df.withColumn('category_name', extract_category_name_udf(df.category))

def main():
    df = spark.read \
        .option(key="delimiter", value=",") \
        .option(key="header", value=True) \
        .csv('src/resources/exo4/sell.csv')

    start_time = time.time()
    _ = add_category_name(df)
    end_time = time.time()
    runtime = end_time - start_time
    print("Runtime python_udf:", runtime, "secs")

