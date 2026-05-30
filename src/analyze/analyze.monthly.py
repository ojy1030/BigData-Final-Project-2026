from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("AnalyzeMonthly").getOrCreate()

cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

monthly_trends = cleaned_df.groupBy("happenMonth").agg(
    count("*").alias("cases_count")
).orderBy("happenMonth")

print("=== [RESULT 2] MONTHLY TRENDS ===")
monthly_trends.show(12, truncate=False)

spark.stop()