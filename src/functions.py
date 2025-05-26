from pyspark.sql.functions import trim, col, month, year, udf, when
from pyspark.sql.types import StringType, IntegerType

def clean_dataframe(df):
    """
    Clean a Spark DataFrame by removing rows with null values and duplicates.
    Args:
        session (SparkSession): Spark session.
        df (DataFrame): DataFrame to clean.

    Returns:
        df_clean: Clean DataFrame.
    """

    # Remove rows with null values in any column
    df = df.dropna(how="any")

    # Remove duplicate rows
    df = df.dropDuplicates()

    # Trim whitespace from string columns
    df = df.select([trim(col).alias(col) if isinstance(df.schema[col].dataType, StringType) else col for col in df.columns])
    return df

def categorize_products(val):
    """
    Categorize products based on their values.
    Args:
        val (float): integer containing product prices.

    Returns:
        Column: New column with a label of 3 possible values.
    """
    if val < 20 : return "Low"
    elif (val >= 20) & (val <= 100) : return "Medium"
    else: return "High"

def transform_dataframe(df_product, df_sales, df_store):
    """
    Transform a Spark DataFrame by cleaning it and applying additional transformations.
    Args:
        session (SparkSession): Spark session.
        df_product (DataFrame): DataFrame with information related with products.
        df_sales (DataFrame): DataFrame with information related with sales.
        df_store (DataFrame): DataFrame with information related with stores.
    Returns:
        df_agg (DataFrame): Dataframe with the total revenue for each store and product category
        df_monthly_insights (DataFrame): Dataframe with the quantity sold for each product category and month
        df_enriched (Dataframe): Combined DataFrame with enriched information from products, sales, and stores.

    Steps:
        1. Unify all DataFrames into a single DataFrame.
        2. Get specific columns for each DataFrame to return.
        3. Integrate UDF function to implement new column.
    
    """
    df_unified = df_sales.join(df_product, ['product_id'], how='left') \
                         .join(df_store, ['store_id'], how='left')
    df_unified = df_unified.withColumn("revenue", df_unified["quantity"] * df_unified["price"])
    df_unified.show()

    df_agg = df_unified.groupBy("store_id", "category").sum("revenue").withColumnRenamed("sum(revenue)","total_revenue")

    df_monthly_insights = df_unified.select("transaction_date", "category", "quantity")
    df_monthly_insights = df_monthly_insights.withColumn("month", month(df_monthly_insights["transaction_date"]))
    df_monthly_insights = df_monthly_insights.withColumn("year", year(df_monthly_insights["transaction_date"]))
    df_monthly_insights = df_monthly_insights.groupBy("year", "month", "category").sum("quantity").withColumnRenamed("sum(quantity)","total_quantity")
    df_monthly_insights = df_monthly_insights.withColumn("total_quantity", col("total_quantity").cast(IntegerType()))

    df_enriched = df_unified.select("transaction_id","store_name","location","product_name","category","quantity","transaction_date", "price")

    categorize_products_udf = udf(categorize_products , StringType())
    df_enriched = df_enriched.withColumn("price_category", categorize_products_udf(col("price")))

    return df_agg, df_monthly_insights, df_enriched
