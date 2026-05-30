from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, regexp_extract, to_date, datediff, when

spark = SparkSession.builder.appName("AnimalPreprocessing").getOrCreate()

# ID 깨짐 방지를 위해 inferSchema=False
df = spark.read.option("header", "true").option("inferSchema", "false").csv("hdfs:///user/maria_dev/animal_project/raw/*.csv")

# 결측치 처리
df = df.fillna({"age": "0", "processState": "알 수 없음", "specialMark": "일반"})

# 분석용 필드 정제
cleaned_df = df \
    .withColumn("desertionNo", col("desertionNo").cast("string")) \
    .withColumn("careRegNo", col("careRegNo").cast("string")) \
    .withColumn("happenYear", col("happenYear").cast("int")) \
    .withColumn("happenMonth", substring(col("happenDt").astype("string"), 5, 2)) \
    .withColumn("birthYear", regexp_extract(col("age"), r"(\d+)", 1).cast("int")) \
    .withColumn("calculated_age", when(col("happenYear").isNotNull() & col("birthYear").isNotNull(), 
                                        col("happenYear") - col("birthYear")).otherwise(0)) \
    .withColumn("cleaned_weight", regexp_extract(col("weight"), r"([0-9.]+)", 1).cast("float")) \
    .withColumn("start_date", to_date(col("noticeSdt"), "yyyyMMdd")) \
    .withColumn("end_date", to_date(col("noticeEdt"), "yyyyMMdd")) \
    .withColumn("stay_days", datediff(col("end_date"), col("start_date"))) \
    .filter(col("stay_days") >= 0)

cleaned_df.write.mode("overwrite").parquet("hdfs:///user/maria_dev/animal_project/processed/")
spark.stop()