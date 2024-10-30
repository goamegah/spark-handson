from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName('hymaia') \
#     .master('local[*]') \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("Jar") \
    .master("local[*]") \
    .config('spark.jars', 'src/resources/exo4/udf.jar') \
    .getOrCreate()