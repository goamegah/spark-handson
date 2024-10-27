from typing import Dict
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from src.fr.hymaia.spark_utils import spark

JOB = "src.fr.hymaia.exo1.spark_wordcount_job"

COUNT_KEY = "count_key"
EXO1_INPUT_PATH = "src/resources/exo1/data.csv"
EXO1_OUTPUT_PATH = "data/exo1/output"

def main():
    inputs = start()
    output = run(inputs)
    end(output)

def start() -> Dict[str, DataFrame]: 
    df = (spark.read.option("header", True).csv(EXO1_INPUT_PATH))
    inputs = {COUNT_KEY: df}
    return inputs

def run(inputs: Dict[str, DataFrame]) -> DataFrame:
    input = inputs[COUNT_KEY]
    return wordcount(input, col_name='text')

def end(output: DataFrame) -> None:
    output.write.mode('overwrite').partitionBy('count').parquet(EXO1_OUTPUT_PATH)
    

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

if __name__ == '__main__':
    main()
