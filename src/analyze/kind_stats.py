# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, avg, round, when

# 스파크 세션 생성 (하이브 연동 제거하여 에러 방지)
spark = SparkSession.builder.appName("AnimalAnalysis-Kind").getOrCreate()

# 정제된 데이터 로드
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

# 데이터 정제 및 파생 변수 생성 로직 (기존 로직 100% 유지)
# 🌟 하이브 매핑 억까를 방지하기 위해 upKindNm 컬럼을 소문자(upkindnm)로 변경
enriched_df = cleaned_df.withColumn("is_mixed", when(col("kindNm").like("%믹스%"), "Mixed").otherwise("Purebred")) \
                         .withColumn("age_group", when(col("calculated_age") <= 2, "Baby(0-2)").when((col("calculated_age") > 2) & (col("calculated_age") <= 7), "Adult(3-7)").otherwise("Senior(8+)")) \
                         .withColumnRenamed("upKindNm", "upkindnm")

# [분석] 품종/믹스여부/연령대별 종합 통계 계산
# 🌟 모든 기준 컬럼을 소문자 규격(upkindnm)으로 묶어 Parquet 저장
kind_stats = enriched_df.groupBy("upkindnm", "is_mixed", "age_group").agg(
    count("*").alias("total_cases"),
    round(avg("cleaned_weight"), 2).cast("string").alias("avg_weight"),
    round(avg("stay_days"), 1).cast("string").alias("avg_stay_days"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("euthanasia_rate"),
    round(expr("sum(case when processState like '%자연사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("natural_death_rate")
).orderBy("upkindnm", "is_mixed", "age_group")

# 🌟 HDFS에 Parquet 형식으로 물리 저장만 수행
hdfs_path_kind = "hdfs:///user/maria_dev/animal_project/mart/kind_stats"
kind_stats.write.mode("overwrite").parquet(hdfs_path_kind)

spark.stop()