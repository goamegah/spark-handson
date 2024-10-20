
# test no_udf

import unittest
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo4.no_udf import (add_total_price_per_category_per_day,
                                       add_total_price_per_category_per_day_last30_days,
                                       add_category_name_using_spark_fun)


class TestNoUdf(unittest.TestCase):
    def test_addCategoryName(self):
        # Given
        given_df = spark.createDataFrame([
            (1, '2020-01-01', 1, 10.0),
            (2, '2020-01-01', 2, 20.0),
            (3, '2020-01-01', 3, 30.0),
            (4, '2020-01-02', 4, 40.0),
            (5, '2020-01-02', 5, 50.0),
            (6, '2020-01-02', 6, 60.0),
            (7, '2020-01-03', 7, 70.0),
            (8, '2020-01-03', 8, 80.0),
            (9, '2020-01-03', 9, 90.0)
        ], ['id', 'date', 'category', 'price'])

        #   expected
        expected_df = spark.createDataFrame([
            (1, '2020-01-01', 1, 10.0, 'food'),
            (2, '2020-01-01', 2, 20.0, 'food'),
            (3, '2020-01-01', 3, 30.0, 'food'),
            (4, '2020-01-02', 4, 40.0, 'food'),
            (5, '2020-01-02', 5, 50.0, 'food'),
            (6, '2020-01-02', 6, 60.0, 'furniture'),
            (7, '2020-01-03', 7, 70.0, 'furniture'),
            (8, '2020-01-03', 8, 80.0, 'furniture'),
            (9, '2020-01-03', 9, 90.0, 'furniture')
        ], ['id', 'date', 'category', 'price', 'category_name'])

        # When
        actual_df = add_category_name_using_spark_fun(given_df)

        # Then
        self.assertEqual(expected_df.columns, actual_df.columns)
        self.assertEqual(expected_df.collect(), actual_df.collect())

    def test_addTotalPricePerCategoryPerDay(self):
        # Given
        given_df = spark.createDataFrame([
            (1, '2020-01-01', 1, 10.0, 'food'),
            (2, '2020-01-01', 2, 20.0, 'food'),
            (3, '2020-01-01', 3, 30.0, 'food'),
            (4, '2020-01-02', 4, 40.0, 'food'),
            (5, '2020-01-02', 5, 50.0, 'food'),
            (6, '2020-01-02', 6, 60.0, 'furniture'),
            (7, '2020-01-03', 7, 70.0, 'furniture'),
            (8, '2020-01-03', 8, 80.0, 'furniture'),
            (9, '2020-01-03', 9, 90.0, 'furniture')
        ], ['id', 'date', 'category', 'price', 'category_name'])

        #   expected
        expected_df = spark.createDataFrame([
            (1, '2020-01-01', 1, 10.0, 'food', 60.0),
            (2, '2020-01-01', 2, 20.0, 'food', 60.0),
            (3, '2020-01-01', 3, 30.0, 'food', 60.0),
            (4, '2020-01-02', 4, 40.0, 'food', 90.0),
            (5, '2020-01-02', 5, 50.0, 'food', 90.0),
            (6, '2020-01-02', 6, 60.0, 'furniture', 60.0),
            (7, '2020-01-03', 7, 70.0, 'furniture', 240.0),
            (8, '2020-01-03', 8, 80.0, 'furniture', 240.0),
            (9, '2020-01-03', 9, 90.0, 'furniture', 240.0)
        ], ['id', 'date', 'category', 'price', 'category_name', 'total_price_per_category_per_day'])

        # When
        actual_df = add_total_price_per_category_per_day(given_df)

        # Then
        self.assertEqual(expected_df.columns, actual_df.columns)
        self.assertEqual(expected_df.collect(), actual_df.collect())

    def test_addTotalPricePerCategoryPerDayLast30Days(self):
        # Given
        given_df = spark.createDataFrame([
            (1, '2020-01-01', 1, 10.0, 'food'),
            (2, '2020-01-01', 2, 20.0, 'food'),
            (3, '2020-01-01', 3, 30.0, 'food'),
            (4, '2020-01-02', 4, 40.0, 'food'),
            (5, '2020-01-02', 5, 50.0, 'food'),
            (6, '2020-01-02', 6, 60.0, 'furniture'),
            (7, '2020-01-03', 7, 70.0, 'furniture'),
            (8, '2020-01-03', 8, 80.0, 'furniture'),
            (9, '2020-01-03', 9, 90.0, 'furniture')
        ], ['id', 'date', 'category', 'price', 'category_name'])

        # expected
        expected_df = spark.createDataFrame([
            (1, '2020-01-01', 1, 10.0, 'food', 10.0),
            (2, '2020-01-01', 2, 20.0, 'food', 30.0),
            (3, '2020-01-01', 3, 30.0, 'food', 60.0),
            (4, '2020-01-02', 4, 40.0, 'food', 100.0),
            (5, '2020-01-02', 5, 50.0, 'food', 150.0),
            (6, '2020-01-02', 6, 60.0, 'furniture', 60.0),
            (7, '2020-01-03', 7, 70.0, 'furniture', 130.0),
            (8, '2020-01-03', 8, 80.0, 'furniture', 210.0),
            (9, '2020-01-03', 9, 90.0, 'furniture', 300.0)
        ], ['id', 'date', 'category', 'price', 'category_name', 'total_price_per_category_per_day_last_30_days'])

        # When
        actual_df = add_total_price_per_category_per_day_last30_days(given_df)

        # Then
        self.assertEqual(expected_df.columns, actual_df.columns)
        self.assertEqual(expected_df.collect(), actual_df.collect())