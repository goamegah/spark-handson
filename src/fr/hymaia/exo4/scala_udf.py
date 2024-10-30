# from pyspark.sql import SparkSession
from src.fr.hymaia.spark_utils import spark
from pyspark.sql.column import Column, _to_java_column, _to_seq
from typing import Dict
from pyspark.sql import DataFrame

# must be defined in the same file
# why ?
# because the udf is defined in the jar file
# and we need to access it via the SparkContext
# which is not available in the main file
# so we need to define the udf in the same file
# and access it via the SparkContext


SUDF_KEY = "sudf_key"
SUDF_INPUT_PATH = 'src/resources/exo4/sell.csv'
SUDF_OUTPUT_DIR = 'data/exo4/sudf/'


def main():
    inputs = start()
    output = add_category_name_using_scala_udf(inputs[SUDF_KEY])
    # output = run(inputs)  # == add_category_name_using_scala_udf
    # end(output)

def start() -> Dict[str, DataFrame]:
    df = spark.read \
        .option(key="delimiter", value=",") \
        .option(key="header", value=True) \
        .csv(SUDF_INPUT_PATH)
    return {SUDF_KEY: df}

def run(inputs: Dict[str, DataFrame]) -> DataFrame:
    df = inputs[SUDF_KEY]
    df = add_category_name_using_scala_udf(df)
    return df

def end(output: DataFrame):
    output.write.mode('overwrite').parquet(SUDF_OUTPUT_DIR)

def get_add_category_name_scala_udf(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, cols=[col], converter=_to_java_column)))

def add_category_name_using_scala_udf(df):
    return df.withColumn('category_name', get_add_category_name_scala_udf(df.category))   

if __name__ == '__main__':
    main()


