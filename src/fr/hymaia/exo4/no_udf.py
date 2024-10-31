from src.fr.hymaia.spark_utils import spark
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from typing import Dict
from pyspark.sql.window import Window

NOUDF_INPUT_PATH = 'src/resources/exo4/sell.csv'
NOUDF_KEY = 'noudf_key'
NOUDF_OUTPUT_DIR = 'data/exo4/noudf/'

def main():
    inputs = start()
    output = add_category_name_using_spark_fun(inputs[NOUDF_KEY])
    # output = run(inputs)
    end(output)

def start() -> Dict[str, DataFrame]:
    df = spark.read \
        .option(key="delimiter", value=",") \
        .option(key="header", value=True) \
        .csv(NOUDF_INPUT_PATH)
    return {NOUDF_KEY: df}

def run(inputs: Dict[str, DataFrame]) -> DataFrame:
    df = inputs[NOUDF_KEY]
    df = add_category_name_using_spark_fun(df)
    df = add_total_price_per_category_per_day(df)
    df = add_total_price_per_category_per_day_last30_days(df)
    return df

def end(output: DataFrame):
    output.write.mode('overwrite').parquet(NOUDF_OUTPUT_DIR)
    
def add_category_name_using_spark_fun(df):
    return df.withColumn(
        'category_name',
        f.when(f.col('category') < 6, value='food')
        .otherwise('furniture')
    )

def add_total_price_per_category_per_day(df):
    window = Window.partitionBy(df.date, df.category_name)
    price_per_partition = f.sum(df.price).over(window)
    return df.withColumn('total_price_per_category_per_day', price_per_partition)


def add_total_price_per_category_per_day_last30_days(df):
    window = Window.partitionBy(df.category_name).orderBy(df.date).rowsBetween(-30, end=0)
    price30days = f.sum(df.price).over(window)
    return df.withColumn('total_price_per_category_per_day_last_30_days', price30days)

if __name__ == '__main__':
    main()
    

