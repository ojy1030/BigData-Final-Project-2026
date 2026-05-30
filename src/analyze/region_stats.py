# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, round

# .enableHiveSupport() 추가
spark = SparkSession.builder.appName("AnimalAnalysis-Region").enableHiveSupport().getOrCreate()
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

# [분석 1] 지역 종합 통계
region_stats = cleaned_df.groupBy("orgNm").agg(
    count("*").alias("total_cases"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("euthanasia_rate"),
    round(expr("sum(case when processState like '%반환%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("return_owner_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) / nullif(sum(case when processState like '%자연사%' then 1 else 0 end), 0)"), 2).alias("euthanasia_to_natural_ratio")
).orderBy(col("total_cases").desc())

# 🌟 Hive 테이블로 저장 (HDFS 파일 저장을 완전 대체)
region_stats.write.mode("overwrite").saveAsTable("hive_region_stats")

# [분석 2] 히트맵용 데이터
top_15 = [row['orgNm'] for row in region_stats.limit(15).select("orgNm").collect()]
heatmap_data = cleaned_df.filter(col("orgNm").isin(top_15)).groupBy("orgNm", "happenMonth").agg(count("*").alias("case_count"))

# 🌟 Hive 테이블로 저장
heatmap_data.write.mode("overwrite").saveAsTable("hive_region_heatmap")

spark.stop()