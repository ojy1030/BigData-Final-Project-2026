# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, round, expr, to_date, datediff

spark = SparkSession.builder.appName("AnimalAnalysis-Timeline-Fixed").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

base_df = cleaned_df.withColumn("start_date", to_date(col("noticeSdt"), "yyyyMMdd")) \
                    .withColumn("end_date", to_date(col("noticeEdt"), "yyyyMMdd")) \
                    .withColumn("calculated_days", datediff(col("end_date"), col("start_date")))

df_with_days = base_df.withColumn(
    "stay_days",
    when((col("calculated_days") >= 0) & (col("calculated_days") <= 365), col("calculated_days"))
    .otherwise(10)
)

# 보호 기간별 입양/안락사 건수 추이
timeline_stats = df_with_days.filter(col("processState").like("%입양%") | col("processState").like("%안락사%")) \
    .withColumn("status_group", when(col("processState").like("%입양%"), "Adoption").otherwise("Euthanasia")) \
    .groupBy("stay_days", "status_group").agg(count("*").alias("animal_count")) \
    .orderBy("stay_days", "status_group")

# CSV 포맷으로 저장 
hdfs_path_timeline = "hdfs:///user/maria_dev/animal_project/mart/timeline_stats"
timeline_stats.write.mode("overwrite").option("header", "false").csv(hdfs_path_timeline)


# 법적 보호 기간(10일) 기준 구간별 통계
legal_zone_stats = df_with_days.withColumn("legal_period_zone",
    when(col("stay_days") <= 10, "01_Within_Legal_Period(0-10d)")
    .when((col("stay_days") > 10) & (col("stay_days") <= 20), "02_Critical_Zone(11-20d)")
    .otherwise("03_Long_Term_Overdue(>20d)")
).groupBy("legal_period_zone").agg(
    count("*").alias("zone_total_cases"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("zone_adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("zone_euthanasia_rate")
).orderBy("legal_period_zone")

# 저장
hdfs_path_legal_zone = "hdfs:///user/maria_dev/animal_project/mart/legal_zone_stats"
legal_zone_stats.write.mode("overwrite").option("header", "false").csv(hdfs_path_legal_zone)

spark.stop()
#