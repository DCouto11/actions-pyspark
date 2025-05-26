import chispa
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType

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
    df_store = spark.createDataFrame(data_store, ["store_id","store_name","location"], schema=schema_stores)

    schema_sales = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("price", DecimalType(5,2), True)
    ])
    data_sales = [(1,1,1,10,'2025-01-01',50.00),(2,1,2,20,'2025-02-01',10.00),(3,1,1,5,'2025-02-01',150.00),(4,1,2,10,'2025-01-01',250.00)]
    df_sales = spark.createDataFrame(data_sales, ["transaction_id","store_id","product_id","quantity","transaction_date","price"], schema=schema_sales)

    schema_products = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True)
    ])
    data_product = [(1, "Product A", "Category 1"), (2, "Product B", "Category 2")]
    df_product = spark.createDataFrame(data_product, ["product_id", "product_name", "category"], schema=schema_products)

    # Transform the DataFrame
    df_agg, df_monthly, df_enriched = transform_dataframe(df_product, df_sales, df_store)

    # Expected DataFrame after transformation
    expected_data_agg = [(1,"Category 1", 1250.00), 
                         (1,"Category 2", 2700.00)]
    expected_df_agg = spark.createDataFrame(expected_data_agg, ["store_id", "category", "total_revenue"])

    expected_data_month = [(2025, 1, "Category 1", 10), 
                           (2025, 2, "Category 2", 20),
                           (2025, 2, "Category 1", 5),
                           (2025, 1, "Category 2", 10)]
    expected_df_month = spark.createDataFrame(expected_data_month, ["year", "month", "category", "total_quantity"])

    expected_data_enrich = [(1,"Test","Avenue Test 3295", "Product A", "Category 1", 10, '2025-01-01', 50.00,"Medium"),
                            (2,"Test","Avenue Test 3295", "Product B", "Category 2", 20, '2025-02-01', 10.00,"Low"),
                            (3,"Test","Avenue Test 3295", "Product A", "Category 1",  5, '2025-02-01',150.00,"High"),
                            (4,"Test","Avenue Test 3295", "Product B", "Category 2", 10, '2025-01-01',250.00,"High")]
    expected_df_enrich = spark.createDataFrame(expected_data_enrich, ["store_id", "category", "total_revenue"])

    # Assert that the transformed DataFrame matches the expected DataFrame
    chispa.assert_df_equality(df_agg, expected_df_agg, ignore_row_order=True)
    chispa.assert_df_equality(df_monthly, expected_df_month, ignore_row_order=True)
    chispa.assert_df_equality(df_enriched, expected_df_enrich, ignore_row_order=True)
