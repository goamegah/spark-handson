from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import run

# TODO : import custom spark code dependencies

CITY_KEY = "city_key"
CLIENT_KEY = "client_key"

def main():
    spark = SparkSession.builder.getOrCreate()

    city_df = spark.read \
        .option(key="header", value="true") \
        .csv("src/resources/exo2/city_zipcode.csv")
    client_df = spark.read \
        .option(key="header", value="true") \
        .csv("src/resources/exo2/clients_bdd.csv")

    inputs = {CLIENT_KEY: client_df, CITY_KEY: city_df}

    df = run(inputs)
    df.show()

if __name__ == '__main__':
    main()