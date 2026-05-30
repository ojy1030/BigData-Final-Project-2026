from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

spark = SparkSession.builder.appName("AnimalPreprocessing").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/maria_dev/animal_project/raw/*.csv")

cleaned_df = df.fillna({"age": "0", "processState": "알 수 없음"})
cleaned_df = cleaned_df.withColumn("happenMonth", substring(col("happenDt").astype("string"), 5, 2))

# 🌟 중요: 전처리 완료된 데이터를 HDFS의 'processed' 폴더에 parquet 포맷으로 저장!
cleaned_df.write.mode("overwrite").parquet("hdfs:///user/maria_dev/animal_project/processed/")
