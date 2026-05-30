# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, round, expr

# 스파크 세션 생성 (하이브 연동 제거하여 에러 방지)
spark = SparkSession.builder.appName("AnimalAnalysis-Timeline").getOrCreate()

# 정제된 데이터 로드
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

# [분석 1] 보호 기간별 입양/안락사 건수 추이
# 🌟 모든 생성 컬럼명이 소문자 규격이라 하이브 매핑 에러 발생 안 함!
timeline_stats = cleaned_df.filter(col("processState").like("%입양%") | col("processState").like("%안락사%")) \
    .withColumn("status_group", when(col("processState").like("%입양%"), "Adoption").otherwise("Euthanasia")) \
    .groupBy("stay_days", "status_group").agg(count("*").alias("animal_count")) \
    .orderBy("stay_days", "status_group")

# 🌟 [분석 1 저장] HDFS에 Parquet 형식으로 물리 저장
hdfs_path_timeline = "hdfs:///user/maria_dev/animal_project/mart/timeline_stats"
timeline_stats.write.mode("overwrite").parquet(hdfs_path_timeline)


# [분석 2] 법적 보호 기간(10일) 기준 구간별 통계
# 🌟 소수점 연산 결과 뒤에전부 .cast("string") 처리 완료된 정석 상태 유지
legal_zone_stats = cleaned_df.withColumn("legal_period_zone",
    when(col("stay_days") <= 10, "01_Within_Legal_Period(0-10d)")
    .when((col("stay_days") > 10) & (col("stay_days") <= 20), "02_Critical_Zone(11-20d)")
    .otherwise("03_Long_Term_Overdue(>20d)")
).groupBy("legal_period_zone").agg(
    count("*").alias("zone_total_cases"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("zone_adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("zone_euthanasia_rate")
).orderBy("legal_period_zone")

# 🌟 [분석 2 저장] HDFS에 Parquet 형식으로 물리 저장
hdfs_path_legal_zone = "hdfs:///user/maria_dev/animal_project/mart/legal_zone_stats"
legal_zone_stats.write.mode("overwrite").parquet(hdfs_path_legal_zone)

spark.stop()