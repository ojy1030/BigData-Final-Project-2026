# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, avg, round, when, to_date, datediff, regexp_extract, lit

# 스파크 세션 생성
spark = SparkSession.builder.appName("AnimalAnalysis-Kind-Fixed").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 정제된 데이터 로드
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

# 나이 환산 및 몸무게 이상치 소탕, stay_days 동적 계산
analysis_df = cleaned_df.withColumn("extracted_age", regexp_extract(col("age"), r"(\d+)", 1).cast("double")) \
                        .withColumn("extracted_weight", regexp_extract(col("weight"), r"([\d.]+)", 1).cast("double")) \
                        .withColumn("start_date", to_date(col("noticeSdt"), "yyyyMMdd")) \
                        .withColumn("end_date", to_date(col("noticeEdt"), "yyyyMMdd"))

# 파생 컬럼 생성 규칙 적용
enriched_df = analysis_df.withColumn(
    "cleaned_age",
    when((col("extracted_age") >= 1900) & (col("extracted_age") <= 2026), lit(2026) - col("extracted_age"))
    .when((col("extracted_age") > 0) & (col("extracted_age") < 100), col("extracted_age"))
    .otherwise(lit(0))
).withColumn(
    "cleaned_weight",
    when((col("extracted_weight") >= 0.1) & (col("extracted_weight") <= 150.0), col("extracted_weight"))
    .otherwise(lit(None).cast("double"))
).withColumn(
    "stay_days",
    when((datediff(col("end_date"), col("start_date")) >= 0) & (datediff(col("end_date"), col("start_date")) <= 365), datediff(col("end_date"), col("start_date")))
    .otherwise(10)
)

# 믹스 여부 및 연령대 그룹화
enriched_df = enriched_df.withColumn("is_mixed", when(col("kindNm").like("%믹스%"), "Mixed").otherwise("Purebred")) \
                        .withColumn("age_group", when(col("cleaned_age") <= 2, "Baby(0-2)").when((col("cleaned_age") > 2) & (col("cleaned_age") <= 7), "Adult(3-7)").otherwise("Senior(8+)")) \
                        .withColumnRenamed("upKindNm", "upkindnm")

# 품종/믹스여부/연령대별 종합 통계 계산
kind_stats = enriched_df.groupBy("upkindnm", "is_mixed", "age_group").agg(
    count("*").alias("total_cases"),
    round(avg("cleaned_weight"), 2).cast("string").alias("avg_weight"),
    round(avg("stay_days"), 1).cast("string").alias("avg_stay_days"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("euthanasia_rate"),
    round(expr("sum(case when processState like '%자연사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("natural_death_rate")
).orderBy("upkindnm", "is_mixed", "age_group")

# 저장
hdfs_path_kind = "hdfs:///user/maria_dev/animal_project/mart/kind_stats"
kind_stats.write.mode("overwrite").option("header", "false").csv(hdfs_path_kind)

spark.stop()
#