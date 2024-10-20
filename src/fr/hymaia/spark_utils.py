from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('hymaia') \
    .master('local[*]') \
    .getOrCreate()