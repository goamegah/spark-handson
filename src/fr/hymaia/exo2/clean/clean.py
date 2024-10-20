import pyspark.sql.functions as f

def keep_adults(df):
    return df.filter(f.col('age') >= 18)

def join_adult_and_city(df1, df2, key):
    return df1.join(df2, key, 'inner')

def add_dept(df):
    return df.withColumn(
        'department',
        f.when(f.col('zip').startswith('20'),
               f.when(f.col("zip") <= "20190", value="2A")
               .otherwise("2B"))
        .otherwise(f.substring(f.col('zip'), pos=1, len=2))
    )