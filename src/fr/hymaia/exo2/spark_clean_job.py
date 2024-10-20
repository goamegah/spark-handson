from src.fr.hymaia.spark_utils import spark
from .clean.clean import keep_adults, join_adult_and_city, add_dept

def clean_job(df_clt, df_cit):
    adults_df = keep_adults(df_clt)
    adults_city_df = join_adult_and_city(adults_df, df_cit, key='zip')
    adults_city_df = add_dept(adults_city_df)
    return adults_city_df

def main():
    client_df = spark.read \
        .option(key="header", value=True) \
        .option(key="delimiter", value=",") \
        .csv("src/resources/exo2/clients_bdd.csv")
    city_df =  spark.read \
        .option(key="header", value=True) \
        .option(key="delimiter", value= ",") \
        .csv("src/resources/exo2/city_zipcode.csv")


    adults_city_df = clean_job(client_df, city_df)
    # adults_city_df.show()
    # adults_city_df.write.mode('overwrite').parquet('data/exo2/output')
