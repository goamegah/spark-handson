from src.fr.hymaia.spark_utils import spark
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import time

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

def main():

    df = spark.read \
        .option(key="delimiter", value=",") \
        .option(key="header", value=True) \
        .csv('src/resources/exo4/sell.csv')

    start_time = time.time()
    _ = add_category_name_using_spark_fun(df)
    end_time = time.time()
    runtime = end_time - start_time
    print(f"\n\nRuntime no_udf: {runtime} secs\n\n")

