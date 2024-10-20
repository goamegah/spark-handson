from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_clean_job import clean_job
import unittest

class CleanTest(unittest.TestCase):
    def testCleanJob(self):
        # given
        client_df = spark.createDataFrame(
            [
                ('Franck', 45, '68150'),
                ('Carl', 43, '32110'),
                ('Cool', 54, '70320'),
                ('Amegah', 13, '68150'),
                ('Godwin', 30, '20190'),
                ('komlan', 12, '68150'),
            ],
            ['name', 'age', 'zip']
        )

        city_df = spark.createDataFrame(
            [
                ('68150', 'SAINT CITY'),
                ('32110', 'VILLERS GRELOT'),
                ('70320', 'VILLE DU PONT'),
                ('20190', 'TOGO'),
            ],
            ['zip', 'city']
        )

        data = [
            ('20190', 'Godwin', 30, 'TOGO', '2A'),
            ('32110', 'Carl', 43, 'VILLERS GRELOT', '32'),
            ('68150', 'Franck', 45, 'SAINT CITY', '68'),
            ('70320', 'Cool', 54, 'VILLE DU PONT', '70')
        ]

        # Define the schema
        columns = ['zip', 'name', 'age', 'city', 'department']

        # Create DataFrame
        expected = spark.createDataFrame(data, columns)

        # when
        actual = clean_job(client_df, city_df)

        # then
        self.assertCountEqual(actual.collect(), expected.collect())




