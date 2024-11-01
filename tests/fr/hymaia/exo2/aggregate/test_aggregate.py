import unittest
import pyspark
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_aggregate_job import run, client_by_dept
from pyspark.sql.types import StructType, StructField, StringType, LongType

AGG_KEY = "adults_city"

class ClientByDeptTest(unittest.TestCase):
    
        def testClientByDept(self):
            # given
            adults_city_df = spark.createDataFrame(
                [
                    ('Franck', 45, '68150', 'SAINT CITY', '68'),
                    ('Carl', 43, '32110', 'VILLERS GRELOT', '32'),
                    ('Cool', 54, '70320', 'VILLE DU PONT', '70'),
                    ('Amegah', 54, '68150', 'ALEYRAC', '68'),
                    ('Godwin', 30, '20190', 'TOGO', '2A'),
                    ('komlan', 30, '68150', 'GABON', '68'),
                ],
                ['name', 'age', 'zip', 'city', 'department']
            )
            expected_schema = StructType([
                StructField('department', StringType(), True),
                StructField('nb_people', LongType(), False),
            ])
            expected_df = spark.createDataFrame(
                data=[
                    ('68', 3),
                    ('2A', 1),
                    ('32', 1),
                    ('70', 1),
                ],
                schema=expected_schema
            )
    
            # when
            aggregated_df = client_by_dept(adults_city_df)
    
            # then
            self.assertEqual(expected_df.schema, aggregated_df.schema)
            self.assertEqual(expected_df.collect(), aggregated_df.collect())
            self.assertEqual(expected_df.columns, aggregated_df.columns)
            self.assertEqual(expected_df.dtypes, aggregated_df.dtypes)
    
        def testInvalideInput(self):
            # GIVEN
            schema = StructType([
                StructField('name', StringType(), True),
                StructField('age', LongType(), True),
                StructField('zip', StringType(), True),
                StructField('city', StringType(), True),
                StructField('department', StringType(), True),
            ])

            df = spark.createDataFrame(
                [
                    ('Franck', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                    ('Carl', 43, '32110', 'VILLERS GRELOT', '32'),
                    ('Cool', 54, '70320', 'VILLE DU PONT', '70'),
                    ('Amegah', 54, '68150', 'ALEYRAC', '68'),
                    ('Godwin', 30, '20190', 'DAKAR', '2A'),
                    ('Komlan', 30, '68150', 'CITY', '68'),
                ],
                schema
            )

            with self.assertRaises(Exception) as context:
                # when
                _ = client_by_dept(df.select("name", "age"))
            self.assertIsInstance(context.exception, pyspark.errors.AnalysisException)


class AggregateJobIntegrationTest(unittest.TestCase):

    def testDifferentSchemaCheck(self):
        # given
        adults_city_df = spark.createDataFrame(
            [
                ('Franck', 45, '68150', 'SAINT CITY', '68'),
                ('Carl', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Cool', 54, '70320', 'VILLE DU PONT', '70'),
                ('Amegah', 54, '68150', 'ALEYRAC', '68'),
                ('Godwin', 30, '20190', 'TOGO', '2A'),
                ('komlan', 30, '68150', 'GABON', '68'),
            ],
            ['name', 'age', 'zip', 'city', 'department']
        )
        expected_df = spark.createDataFrame(
            [
                ('68', 3),
                ('2A', 1),
                ('32', 1),
                ('70', 1),
            ],
            ['department', 'nb_people']
        )

        inputs = {AGG_KEY: adults_city_df}

        # when
        aggregated_df = run(inputs)

        # then
        with self.assertRaises(AssertionError) as context:
            self.assertEqual(expected_df.schema, aggregated_df.schema)
        self.assertEqual(type(context.exception), AssertionError)

    def testSameSchemaCheck(self):
        # given
        schema = StructType([
            StructField('name', StringType(), True),
            StructField('age', LongType(), True),
            StructField('zip', StringType(), True),
            StructField('city', StringType(), True),
            StructField('department', StringType(), True),
        ])
        adults_city_df = spark.createDataFrame(
            [
                ('Franck', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                ('Carl', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Cool', 54, '70320', 'VILLE DU PONT', '70'),
                ('Amegah', 54, '68150', 'ALEYRAC', '68'),
                ('Godwin', 30, '20190', 'DAKAR', '2A'),
                ('Komlan', 30, '68150', 'CITY', '68'),
            ],
            schema
        )

        expected_schema = StructType([
            StructField('department', StringType(), True),
            StructField('nb_people', LongType(), False),
        ])
        expected_df = spark.createDataFrame(
            data=[
                ('68', 3),
                ('2A', 1),
                ('32', 1),
                ('70', 1),
            ],
            schema=expected_schema
        )
        
        inputs = {AGG_KEY: adults_city_df}

        # when
        aggregated_df = run(inputs)

        # then
        self.assertEqual(expected_df.schema, aggregated_df.schema)
        self.assertEqual(expected_df.collect(), aggregated_df.collect())
        self.assertEqual(expected_df.columns, aggregated_df.columns)
        self.assertEqual(expected_df.dtypes, aggregated_df.dtypes)

