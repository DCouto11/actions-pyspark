from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from functions import (clean_dataframe, transform_dataframe)

# Create spark session
spark = SparkSession.builder \
 .appName("CSV to Parquet") \
 .getOrCreate()

# Creating schemas for CSV files
schema_products = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True)
])
schema_sales = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("price", FloatType(), True)
])
schema_stores = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("store_name", StringType(), True),
    StructField("location", StringType(), True)
])

# Loading CSV files with schemas into DataFrames
df_products = spark.read.option("header", "true").option("delimiter",";").schema(schema_products).csv("files/products.csv")
df_sales = spark.read.option("header", "true").option("delimiter",";").schema(schema_sales).csv("files/sales.csv")
df_stores = spark.read.option("header", "true").option("delimiter",";").schema(schema_stores).csv("files/stores.csv")

# Cleaning DataFrames (removing nulls, duplicates, trimming strings)
df_prod_clean = clean_dataframe(df_products)
df_sales_clean = clean_dataframe(df_sales)
df_stores_clean = clean_dataframe(df_stores)

# DataFrame transformations (sales aggregation, monthly sales insights)
df_agg_sales, df_monthly_sales, df_enriched_sales = transform_dataframe(df_prod_clean, df_sales_clean, df_stores_clean)

# Saving DataFrames in CSV format
df_agg_sales.write.mode("overwrite").csv("output/agg_sales.csv", header=True)

# Saving DataFrames in Parquet format
df_enriched_sales.write.partitionBy('category','transaction_date').mode("overwrite").parquet("output/enriched_sales.parquet")

# Ending Spark session
spark.stop()
