import chispa
import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, DoubleType

from src.functions import (clean_dataframe,transform_dataframe)

spark = SparkSession.builder.appName("Test Functions") \
    .master("local") \
    .getOrCreate()

def test_clean_dataframe():
    # Create a sample DataFrame
    data = [("Alice        ", 1), ("Bob", None), ("        Charlie", 3), (None, 4)]
    df = spark.createDataFrame(data, ["name", "value"])

    # Clean the DataFrame
    cleaned_df = clean_dataframe(df)

    # Expected DataFrame after cleaning
    expected_data = [("Alice", 1), ("Charlie", 3)]
    expected_df = spark.createDataFrame(expected_data, ["name", "value"])

    # Assert that the cleaned DataFrame matches the expected DataFrame
    chispa.assert_df_equality(cleaned_df, expected_df, ignore_row_order=True)


def test_transform_dataframe():
    # Create all sample DataFrame
    schema_stores = StructType([
        StructField("store_id", IntegerType(), True),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True)
    ])
    data_store = [(1, "Test", "Avenue Test 3295")]
    df_store = spark.createDataFrame(data_store, schema=schema_stores)

    schema_sales = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("price", FloatType(), True)
    ])
    data_sales = [(1,1,1,10,datetime.date(2025,1,1),50.00),
                  (2,1,2,20,datetime.date(2025,2,1),10.00),
                  (3,1,1,5,datetime.date(2025,2,1),150.00),
                  (4,1,2,10,datetime.date(2025,1,1),250.00)]
    df_sales = spark.createDataFrame(data_sales, schema=schema_sales)

    schema_products = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True)
    ])
    data_product = [(1, "Product A", "Category 1"), (2, "Product B", "Category 2")]
    df_product = spark.createDataFrame(data_product, schema=schema_products)

    # Transform the DataFrame
    df_agg, df_monthly, df_enriched = transform_dataframe(df_product, df_sales, df_store)

    # Expected DataFrame after transformation
    expected_data_agg = [(1,"Category 1", 1250.00), 
                         (1,"Category 2", 2700.00)]
    
    schema_expected_agg = StructType([
        StructField("store_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("total_revenue", DoubleType(), True)
    ])
    expected_df_agg = spark.createDataFrame(expected_data_agg, schema=schema_expected_agg)

    expected_data_month = [(2025, 1, "Category 1", 10), 
                           (2025, 2, "Category 2", 20),
                           (2025, 2, "Category 1", 5),
                           (2025, 1, "Category 2", 10)]
    schema_expected_month = StructType([
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("total_quantity", IntegerType(), True)
    ])
    expected_df_month = spark.createDataFrame(expected_data_month, schema=schema_expected_month)

    expected_data_enrich = [(1,"Test","Avenue Test 3295", "Product A", "Category 1", 10, datetime.date(2025,1,1), 50.00,"Medium"),
                            (2,"Test","Avenue Test 3295", "Product B", "Category 2", 20, datetime.date(2025,2,1), 10.00,"Low"),
                            (3,"Test","Avenue Test 3295", "Product A", "Category 1",  5, datetime.date(2025,2,1),150.00,"High"),
                            (4,"Test","Avenue Test 3295", "Product B", "Category 2", 10, datetime.date(2025,1,1),250.00,"High")]
    schema_expected_enrich = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("price", FloatType(), True),
        StructField("price_category", StringType(), True)
    ])
    expected_df_enrich = spark.createDataFrame(expected_data_enrich,schema=schema_expected_enrich)

    # Assert that the transformed DataFrame matches the expected DataFrame
    chispa.assert_df_equality(df_agg, expected_df_agg, ignore_row_order=True)
    chispa.assert_df_equality(df_monthly, expected_df_month, ignore_row_order=True)
    chispa.assert_df_equality(df_enriched, expected_df_enrich, ignore_row_order=True)
