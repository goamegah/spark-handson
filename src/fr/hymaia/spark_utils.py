from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hymaia") \
    .master("local[*]") \
    .config('spark.jars', 'src/resources/exo4/udf.jar') \
    .getOrCreate()