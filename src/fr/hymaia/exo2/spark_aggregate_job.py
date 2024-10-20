from .aggregate.aggregate import client_by_dept
from src.fr.hymaia.utils.utils import move_output, remove_dirs
from src.fr.hymaia.spark_utils import spark

def aggregate(df):
    df_client_by_dept = client_by_dept(df)
    return df_client_by_dept

def main():

    df = spark.read.parquet('data/exo2/output')

    df_client_by_dept = aggregate(df)

    df_client_by_dept.write.mode('overwrite').csv('data/exo2/aggregate', header=True)
    move_output('data/exo2/aggregate', 'data/exo2/')
    remove_dirs('data/exo2/aggregate')

    # TODO: Rendre le reste du code propre
    # clean_output('data/exo2/output')