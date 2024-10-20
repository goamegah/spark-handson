import sys
from src.fr.hymaia.exo2.spark_clean_job import clean_job
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# TODO : import custom spark code dependencies

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    job.init(args['JOB_NAME'], args)

    df_city = spark.read \
        .option(key="header", value="true") \
        .csv("s3://goamegah-spark-handson-bucket/resources/exo2/city_zipcode.csv")

    df_client = spark.read \
        .option(key="header", value="true") \
        .csv("s3://goamegah-spark-handson-bucket/resources/exo2/clients_bdd.csv")

    adults_city_df = clean_job(df_client, df_city)

    # write data on s3
    adults_city_df.write.mode("overwrite").parquet("s3://goamegah-spark-handson-bucket/data/exo2/output/clean")

    # TODO : call function to run spark transformations

    job.commit()