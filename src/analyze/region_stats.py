# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, round

# 스파크 세션 기본 생성
spark = SparkSession.builder.appName("AnimalAnalysis-Region").getOrCreate()

# 정제된 데이터 로드
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

# [분석 1] 지역 종합 통계 계산 (모든 소수점 결과를 .cast("string")으로 안전하게 변환)
region_stats = cleaned_df.groupBy("orgNm").agg(
    count("*").alias("total_cases"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("euthanasia_rate"),
    round(expr("sum(case when processState like '%반환%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("return_owner_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) / nullif(sum(case when processState like '%자연사%' then 1 else 0 end), 0)"), 2).cast("string").alias("euthanasia_to_natural_ratio")
).orderBy(col("total_cases").desc())

# HDFS에 Parquet 물리 파일 저장 (데이터 엎어치기)
hdfs_path_stats = "hdfs:///user/maria_dev/animal_project/mart/region_stats"
region_stats.write.mode("overwrite").parquet(hdfs_path_stats)

# [분석 2] 히트맵용 데이터 계산 (원래 정수형 데이터라 그대로 유지)
top_15 = [row['orgNm'] for row in region_stats.limit(15).select("orgNm").collect()]
heatmap_data = cleaned_df.filter(col("orgNm").isin(top_15)).groupBy("orgNm", "happenMonth").agg(count("*").alias("case_count"))

# HDFS에 Parquet 물리 파일 저장
hdfs_path_heatmap = "hdfs:///user/maria_dev/animal_project/mart/region_heatmap"
heatmap_data.write.mode("overwrite").parquet(hdfs_path_heatmap)

spark.stop()