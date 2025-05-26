from pyspark.sql import SparkSession
from functions import clean_dataframe, transform_dataframe

# Create spark session
spark = SparkSession.builder \
 .appName("CSV to Parquet") \
 .getOrCreate()

# Loading CSV files into DataFrames
df_products = spark.read.option("header", "true").csv("files/products.csv")
df_sales = spark.read.option("header", "true").csv("files/sales.csv")
df_stores = spark.read.option("header", "true").csv("files/stores.csv")

# Cleaning DataFrames (removing nulls, duplicates, trimming strings)
df_prod_clean = clean_dataframe(spark, df_products)
df_sales_clean = clean_dataframe(spark, df_sales)
df_stores_clean = clean_dataframe(spark, df_stores)

# DataFrame transformations (sales aggregation, monthly sales insights)
df_agg_sales, df_monthly_sales, df_enriched_sales = transform_dataframe(spark, df_prod_clean, df_sales_clean, df_stores_clean)

# Saving DataFrames in CSV format
df_agg_sales.write.mode("overwrite").csv("output/agg_sales.csv", header=True)

# Saving DataFrames in Parquet format
df_enriched_sales.write.partitionBy('category','transaction_date').mode("overwrite").parquet("output/enriched_sales.parquet")

# Ending Spark session
spark.stop()
