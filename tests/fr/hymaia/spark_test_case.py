from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("unit test") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()


