from src.fr.hymaia.spark_utils import spark
from src.fr.hymaia.exo2.clean.clean import keep_adults, join_adult_and_city, add_dept
from pyspark.sql import DataFrame
from typing import Dict

JOB = "src.fr.hymaia.exo2.spark_clean_job"

CLIENT_BDD_PATH = "src/resources/exo2/clients_bdd.csv"
CITY_BDD_PATH = "src/resources/exo2/city_zipcode.csv"
CLEAN_OUTPUT_DIR = "data/exo2/clean/"
CITY_KEY = "city_key"
CLIENT_KEY = "client_key"

def main():
    inputs = start()
    output = run(inputs)
    end(output)

def start() -> Dict[str, DataFrame]:
    client_df = spark.read \
        .option(key="header", value=True) \
        .option(key="delimiter", value=",") \
        .csv(CLIENT_BDD_PATH)
    
    city_df =  spark.read \
        .option(key="header", value=True) \
        .option(key="delimiter", value= ",") \
        .csv(CITY_BDD_PATH)
    
    inputs = {CLIENT_KEY: client_df, CITY_KEY: city_df}
    return inputs

def run(inputs: Dict[str, DataFrame]) -> DataFrame:
    client_df, city_df = inputs[CLIENT_KEY], inputs[CITY_KEY]
    adults_df = keep_adults(client_df)
    adults_city_df = join_adult_and_city(adults_df, city_df, key='zip')
    adults_city_df = add_dept(adults_city_df)
    return adults_city_df

def end(output: DataFrame):
    output.write.mode('overwrite').parquet(CLEAN_OUTPUT_DIR)

if __name__ == '__main__':
    main()
