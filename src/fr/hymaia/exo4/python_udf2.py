from src.fr.hymaia.spark_utils import spark
import pyspark.sql.functions as f
from typing import Dict
from pyspark.sql import DataFrame

UDF2_KEY = 'udf2_key'
UDF2_INPUT_PATH = 'src/resources/exo4/sell.csv'
UDF2_OUTPUT_DIR = 'data/exo4/udf2/'


def main():
    inputs = start()
    output = add_category_name(inputs[UDF2_KEY])
    # output = run(inputs)  # -> add_category_name
    # end(output)

def start() -> Dict[str, DataFrame]:
    df = spark.read \
        .option(key="delimiter", value=",") \
        .option(key="header", value=True) \
        .csv(UDF2_INPUT_PATH)
    return {UDF2_KEY: df}

def end(output: DataFrame):
    output.write.mode('overwrite').parquet(UDF2_OUTPUT_DIR)

def run(inputs: Dict[str, DataFrame]) -> DataFrame:
    df = inputs[UDF2_KEY]
    df = add_category_name(df)
    return df

@f.udf('string')
def extract_category_name_udf(category_id):
    return 'food' if int(category_id) < 6 else 'furniture'

def add_category_name(df):
    return df.withColumn('category_name', extract_category_name_udf(df.category))

if __name__ == '__main__':
    main()

