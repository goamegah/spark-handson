import pyspark.sql.functions as f
from src.fr.hymaia.utils.spark_utils import spark

def main():
    df = (spark.read.option("header", True).csv("src/resources/exo1/data.csv"))
    df_wordcount = wordcount(df, col_name='text')
    df_wordcount.write.mode('overwrite').partitionBy('count').parquet('data/exo1/output')
    df_wordcount.show(n=10)


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
