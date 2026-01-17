import os
import sys
from pyspark.sql import SparkSession

os.environ["SPARK_HOME"] = r"C:\Spark"
os.environ["JAVA_HOME"] = r"C:\Java"
os.environ["PATH"] = os.path.join(os.environ["SPARK_HOME"], "bin") + os.pathsep + \
                    os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + \
                    os.environ["PATH"]

try:
    spark = SparkSession.builder.appName("Test").master("local[1]").getOrCreate()
    print(f"Spark Version: {spark.version}")
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "val"])
    df.show()
    spark.stop()
    print("Test Successful!")
except Exception as e:
    print(f"Test Failed: {e}")
