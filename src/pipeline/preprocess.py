# -*- coding: utf-8 -*
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, regexp_extract, to_date, datediff, when

spark = SparkSession.builder.appName("AnimalPreprocessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("mode", "DROPMALFORMED") \
    .csv("hdfs:///user/maria_dev/animal_project/raw/raw_animals.csv")

# 결측치 처리 안정화
df = df.fillna({"age": "0", "processState": "알 수 없음", "specialMark": "일반", "weight": "0"})

# 분석용 필드 분산 클러스터 정제
cleaned_df = df \
    .withColumn("desertionNo", col("desertionNo").cast("string")) \
    .withColumn("careRegNo", col("careRegNo").cast("string")) \
    .withColumn("happenYear", substring(col("happenDt").astype("string"), 1, 4).cast("int")) \
    .withColumn("happenMonth", substring(col("happenDt").astype("string"), 5, 2)) \
    .withColumn("birthYear", regexp_extract(col("age"), r"(\d+)", 1).cast("int")) \
    .withColumn("calculated_age", when(col("happenYear").isNotNull() & col("birthYear").isNotNull(), 
                                        col("happenYear") - col("birthYear")).otherwise(0)) \
    .withColumn("cleaned_weight", regexp_extract(col("weight"), r"([0-9.]+)", 1).cast("float")) \
    .withColumn("start_date", to_date(col("noticeSdt"), "yyyyMMdd")) \
    .withColumn("end_date", to_date(col("noticeEdt"), "yyyyMMdd")) \
    .withColumn("stay_days", datediff(col("end_date"), col("start_date"))) \
    .filter(col("stay_days") >= 0)

# 최종 마트 연산용 압축 저장
cleaned_df.write.mode("overwrite").parquet("hdfs:///user/maria_dev/animal_project/processed/")
spark.stop()
#