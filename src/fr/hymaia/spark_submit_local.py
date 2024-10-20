from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import clean_job

# TODO : import custom spark code dependencies

def main():
    spark = SparkSession.builder.getOrCreate()

    df_city = spark.read \
        .option(key="header", value="true") \
        .csv("src/resources/exo2/city_zipcode.csv")
    df_client = spark.read \
        .option(key="header", value="true") \
        .csv("src/resources/exo2/clients_bdd.csv")

    df = clean_job(df_client, df_city)
    df.show()

if __name__ == '__main__':
    main()