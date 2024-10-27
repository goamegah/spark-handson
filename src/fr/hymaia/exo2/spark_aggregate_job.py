from pyspark.sql import DataFrame
from src.fr.hymaia.exo2.aggregate.aggregate import client_by_dept
from src.fr.hymaia.utils.utils import move_csv_files, remove_files, make_dirs
from src.fr.hymaia.utils.spark_utils import spark
from typing import Dict

CLEAN_OUTPUT_DIR = "data/exo2/clean/"
AGG_OUTPUT_DIR = "data/exo2/agg/"
TMP_DIR = 'data/exo2/tmp/'
AGG_KEY = "adults_city"

def main():
    input = start()
    output = run(input)
    end(output)

def start() -> Dict[str, DataFrame]: 
    adults_city_df = spark.read.parquet(CLEAN_OUTPUT_DIR)
    inputs = {AGG_KEY: adults_city_df}
    return inputs

def run(inputs: Dict[str, DataFrame]) -> DataFrame:
    input = inputs[AGG_KEY]
    client_by_dept_df = client_by_dept(input)
    return client_by_dept_df

def end(output) -> None:
    make_dirs(AGG_OUTPUT_DIR)
    output.write.mode('overwrite').csv(TMP_DIR, header=True)
    move_csv_files(TMP_DIR, AGG_OUTPUT_DIR)
    remove_files(TMP_DIR)
    remove_files(CLEAN_OUTPUT_DIR)

if __name__ == '__main__':
    main()