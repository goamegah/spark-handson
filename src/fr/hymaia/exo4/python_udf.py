from src.fr.hymaia.spark_utils import spark
import pyspark.sql.functions as f
import time
from typing import Dict
from pyspark.sql import DataFrame
from pyspark import StorageLevel



UDF_KEY = 'udf_key'
UDF_INPUT_PATH = 'src/resources/exo4/sell.csv'
UDF_OUTPUT_DIR = 'data/exo4/udf/'

def main():
    inputs = start()
    output = add_category_name(inputs[UDF_KEY])
    # output = run(inputs)  # -> add_category_name
    end(output)

def start() -> Dict[str, DataFrame]:
    df = (
        spark
        .read
        .option(key="delimiter", value=",")
        .option(key="header", value=True)
        .csv(UDF_INPUT_PATH)
        .persist(StorageLevel.MEMORY_ONLY)
    )
    return {UDF_KEY: df}

def end(output: DataFrame):
    # output.write.mode('overwrite').parquet(UDF_OUTPUT_DIR)
    # output.groupby('category_name').count().show()
    output.groupby('category_name').count().count()


def run(inputs: Dict[str, DataFrame]) -> DataFrame:
    df = inputs[UDF_KEY]
    df = add_category_name(df)
    return df

def add_category_name(df):
    @f.udf('string')
    def extract_category_name_udf(category_id):
        return 'food' if int(category_id) < 6 else 'furniture'
    return df.withColumn('category_name', extract_category_name_udf(df.category))

if __name__ == '__main__':
    main()

