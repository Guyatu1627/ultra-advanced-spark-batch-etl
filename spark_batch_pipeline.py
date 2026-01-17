import os
import sys
import logging
import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as sum_, avg, count, broadcast, lit, coalesce, row_number, expr
from pyspark.sql.types import DoubleType, IntegerType, StringType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pandas as pd

# Set Spark & Java paths
os.environ["SPARK_HOME"] = r"C:\Spark"
os.environ["JAVA_HOME"] = r"C:\Java"
os.environ["HADOOP_HOME"] = os.environ["SPARK_HOME"]
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Guyatu\project11_spark_batch_pipeline\venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Guyatu\project11_spark_batch_pipeline\venv\Scripts\python.exe"
os.environ["PATH"] = os.path.join(os.environ["SPARK_HOME"], "bin") + os.pathsep + \
                    os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + \
                    os.environ["PATH"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    spark = (SparkSession.builder
             .appName("Ultra-Advanced Spark Batch ETL – Project 11")
             .master("local[*]")
             .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    
    logger.info(f"Spark {spark.version} initialized – AQE, Delta Lake, skew handling enabled")
    return spark

def load_data(spark):
    path = "data/raw/transactions_5M.csv"
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("mergeSchema", "true")
          .csv(path))
    
    # Clean & enrich
    df = df.withColumn("price", coalesce(col("price").cast(DoubleType()), lit(0.0)))
    df = df.withColumn("quantity", coalesce(col("quantity").cast(IntegerType()), lit(1)))
    df = df.withColumn("timestamp", F.to_timestamp(col("timestamp")))
    df = df.withColumn("processed_at", F.current_timestamp())
    
    logger.info(f"Loaded & cleaned {df.count():,} rows")
    return df

def ultra_advanced_transformations(df):
    df.cache()  # Cache for multiple passes
    
    # Custom UDF – tiered discount logic
    @F.udf(returnType=DoubleType())
    def tiered_discount(price, quantity, code):
        base = price * quantity
        if code == 'FLASH30':
            return base * 0.7
        elif code == 'WINTER15':
            return base * 0.85
        elif code == 'NEWUSER':
            return base * 0.9
        return base
    
    df = df.withColumn("discounted_revenue", tiered_discount(col("price"), col("quantity"), col("discount_code")))
    
    # Rolling 30-day revenue per user (advanced window)
    window_30d = Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(-29, 0)
    df = df.withColumn("rolling_30d_revenue", sum_("discounted_revenue").over(window_30d))
    
    # Broadcast small dimension (country stats)
    country_stats = df.groupBy("country").agg(
        count("*").alias("txn_count"),
        avg("price").alias("country_avg_price")
    ).filter(col("txn_count") < 200000)  # small enough to broadcast
    df = df.join(broadcast(country_stats), "country", "left_outer")
    
    # Final multi-level aggregation + ranking
    agg_df = (df.groupBy("category", "country", "user_id")
              .agg(
                  sum_("quantity").alias("total_quantity"),
                  sum_("discounted_revenue").alias("total_revenue"),
                  avg("price").alias("avg_price"),
                  count("*").alias("txn_count"),
                  F.max("rolling_30d_revenue").alias("max_rolling_revenue"),
                  F.first("country_avg_price").alias("country_avg_price")
              )
              .withColumn("revenue_per_txn", col("total_revenue") / col("txn_count"))
              .withColumn("rank", row_number().over(Window.partitionBy("category").orderBy(F.desc("total_revenue"))))
              .filter(col("rank") <= 10))  # top 10 per category
    
    logger.info("Ultra-advanced transformations complete")
    return agg_df

def save_to_delta(agg_df):
    delta_path = "data/delta/analytics_sales"
    
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("old").merge(
            agg_df.alias("new"),
            "old.category = new.category AND old.country = new.country AND old.user_id = new.user_id"
        ).whenMatchedUpdateAll(
            condition="old.total_revenue < new.total_revenue"
        ).whenNotMatchedInsertAll().execute()
        logger.info("Delta Lake MERGE completed")
    else:
        (agg_df.write
         .format("delta")
         .mode("overwrite")
         .partitionBy("category", "country")
         .option("overwriteSchema", "true")
         .save(delta_path))
        logger.info("Delta Lake table created with partitioning")
    
    # Time travel demo
    spark.read.format("delta").option("versionAsOf", 0).load(delta_path).limit(5).show()
    logger.info("Time travel demo completed")

def main():
    global spark
    spark = create_spark_session()
    
    try:
        raw_df = load_data(spark)
        agg_df = ultra_advanced_transformations(raw_df)
        save_to_delta(agg_df)
        
        logger.info("PROJECT 11 COMPLETE – Ultra-Advanced Spark Batch Pipeline")
        logger.info("Delta table ready at: data/delta/analytics_sales")
        logger.info("Run in spark-shell: spark.sql('SELECT * FROM delta.`data/delta/analytics_sales` LIMIT 10').show()")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()