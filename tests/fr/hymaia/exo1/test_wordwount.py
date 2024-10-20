from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo1.spark_wordcount_job import wordcount
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_wordcount(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(text='bonjour je suis un test unitaire'),
                Row(text='bonjour suis test')
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(word='bonjour', count=2),
                Row(word='je', count=1),
                Row(word='suis', count=2),
                Row(word='un', count=1),
                Row(word='test', count=2),
                Row(word='unitaire', count=1),
            ]
        )

        actual = wordcount(input, col_name='text')

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.columns, expected.columns)