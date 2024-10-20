import pyspark.sql.functions as f

def client_by_dept(df):
    return df \
        .groupBy('department') \
        .agg(f.count('name').alias('nb_people')) \
        .orderBy(f.desc('nb_people'), f.asc('department'))