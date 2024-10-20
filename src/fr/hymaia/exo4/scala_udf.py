from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time

# must be defined in the same file
# why ?
# because the udf is defined in the jar file
# and we need to access it via the SparkContext
# which is not available in the main file
# so we need to define the udf in the same file
# and access it via the SparkContext

spark = SparkSession.builder \
    .appName("Jar") \
    .master("local[*]") \
    .config('spark.jars', 'src/resources/exo4/udf.jar') \
    .getOrCreate()

def get_add_category_name_scala_udf(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, cols=[col], converter=_to_java_column)))

def add_category_name_using_scala_udf(df):
    return df.withColumn('category_name', get_add_category_name_scala_udf(df.category))

def main():
    df = spark.read \
        .option(key="delimiter", value=",") \
        .option(key="header", value=True) \
        .csv('src/resources/exo4/sell.csv')

    start_time = time.time()
    _ = add_category_name_using_scala_udf(df)
    end_time = time.time()
    runtime = end_time - start_time
    print("Runtime scala_udf:", runtime, "secs")


