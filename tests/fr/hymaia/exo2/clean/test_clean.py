from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_clean_job import run, add_dept, join_adult_and_city, keep_adults
import unittest

CITY_KEY = "city_key"
CLIENT_KEY = "client_key"

class AddDeptTest(unittest.TestCase):
    def testAddDept(self):
        # given
        df = spark.createDataFrame(
            [
                ('Franck', 45, '68150', 'SAINT CITY'),
                ('Carl', 43, '32110', 'VILLERS GRELOT'),
                ('Cool', 54, '70320', 'VILLE DU PONT'),
                ('Godwin', 30, '20190', 'TOGO'),
            ],
            ['name', 'age', 'zip', 'city']
        )

        data = [
            ('Franck', 45, '68150', 'SAINT CITY', '68'),
            ('Carl', 43, '32110', 'VILLERS GRELOT', '32'),
            ('Cool', 54, '70320', 'VILLE DU PONT', '70'),
            ('Godwin', 30, '20190', 'TOGO', '2A'),
        ]

        # Define the schema
        columns = ['name', 'age', 'zip', 'city', 'department']

        # Create DataFrame
        expected = spark.createDataFrame(data, columns)

        # when
        actual = add_dept(df)

        # then
        self.assertCountEqual(actual.collect(), expected.collect())

class KeepAdultsTest(unittest.TestCase):
    def testKeepAdults(self):
        # given
        df = spark.createDataFrame(
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

        data = [
            ('Franck', 45, '68150'),
            ('Carl', 43, '32110'),
            ('Cool', 54, '70320'),
            ('Godwin', 30, '20190'),
        ]

        # Define the schema
        columns = ['name', 'age', 'zip']

        # Create DataFrame
        expected = spark.createDataFrame(data, columns)

        # when
        actual = keep_adults(df)

        # then
        self.assertCountEqual(actual.collect(), expected.collect())

class JoinAdultAndCityTest(unittest.TestCase):
    def testJoinAdultAndCity(self):
        # given
        adults_df = spark.createDataFrame(
            [
                ('Franck', 45, '68150'),
                ('Carl', 43, '32110'),
                ('Cool', 54, '70320'),
                ('Godwin', 30, '20190'),
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
            ('68150', 'Franck', 45, 'SAINT CITY'),
            ('32110', 'Carl', 43, 'VILLERS GRELOT'),
            ('70320', 'Cool', 54, 'VILLE DU PONT'),
            ('20190', 'Godwin', 30, 'TOGO'),
        ]

        # Define the schema
        columns = ['zip', 'name', 'age', 'city']

        # Create DataFrame
        expected = spark.createDataFrame(data, columns)

        # when
        actual = join_adult_and_city(adults_df, city_df, key='zip')

        # then
        self.assertCountEqual(actual.collect(), expected.collect())

class CleanRunTest(unittest.TestCase):
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

        inputs = {CLIENT_KEY: client_df, CITY_KEY: city_df}
        
        # when
        actual = run(inputs)

        # then
        self.assertCountEqual(actual.collect(), expected.collect())




