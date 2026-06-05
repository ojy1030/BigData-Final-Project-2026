# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, round

spark = SparkSession.builder.appName("AnimalAnalysis-Region").getOrCreate()

cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

region_df = cleaned_df.withColumnRenamed("orgNm", "orgnm")

# 지역 종합 통계 계산
region_stats = region_df.groupBy("orgnm").agg(
    count("*").alias("total_cases"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("euthanasia_rate"),
    round(expr("sum(case when processState like '%반환%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("return_owner_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) / nullif(sum(case when processState like '%자연사%' then 1 else 0 end), 0)"), 2).cast("string").alias("euthanasia_to_natural_ratio")
).orderBy(col("total_cases").desc())

# Parquet 대신 CSV 포맷으로 저장
hdfs_path_stats = "hdfs:///user/maria_dev/animal_project/mart/region_stats"
region_stats.write.mode("overwrite").option("header", "false").csv(hdfs_path_stats)

# 히트맵용 데이터 계산 
top_15 = [row['orgnm'] for row in region_stats.limit(15).select("orgnm").collect()]

heatmap_data = region_df.filter(col("orgnm").isin(top_15)) \
    .withColumnRenamed("happenMonth", "happenmonth") \
    .groupBy("orgnm", "happenmonth").agg(count("*").alias("case_count"))

# 저장
hdfs_path_heatmap = "hdfs:///user/maria_dev/animal_project/mart/region_heatmap"
heatmap_data.write.mode("overwrite").option("header", "false").csv(hdfs_path_heatmap)

spark.stop()