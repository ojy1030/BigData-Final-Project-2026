# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, count, avg, round

# 스파크 세션 생성 (하이브 연동 제거하여 에러 방지)
spark = SparkSession.builder.appName("AnimalAnalysis-Keyword").getOrCreate()

# 정제된 데이터 로드
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

# 특징(specialMark) 기반 핵심 키워드 분류 파생 변수 생성 (기존 로직 100% 유지)
# 🌟 detailed_trait 컬럼명은 이미 전부 소문자라 하이브 매핑 에러가 나지 않음!
keyword_df = cleaned_df.withColumn("detailed_trait",
    when(col("specialMark").like("%사나움%") | col("specialMark").like("%입질%") | col("specialMark").like("%경계%") | col("specialMark").like("%짖음%"), "Behavioral_Issue")
    .when(col("specialMark").like("%피부병%") | col("specialMark").like("%골절%") | col("specialMark").like("%외상%") | col("specialMark").like("%질병%"), "Medical_Issue")
    .when(col("specialMark").like("%온순%") | col("specialMark").like("%애교%") | col("specialMark").like("%친화%") | col("specialMark").like("%사람좋아%"), "Socialized")
    .otherwise("Normal_Status")
)

# [분석] 유기동물 성향 키워드별 종합 통계 계산
# 소수점 연산 결과 뒤에 전부 .cast("string")을 붙여 하이브 타입 충돌 억까 차단
keyword_stats = keyword_df.groupBy("detailed_trait").agg(
    count("*").alias("total_cases"),
    round(avg("stay_days"), 1).cast("string").alias("avg_stay_days"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("euthanasia_probability")
).orderBy("avg_stay_days") # 🌟 직관적인 정렬 방식으로 통일

# 🌟 HDFS에 Parquet 형식으로 물리 저장만 수행
hdfs_path_keyword = "hdfs:///user/maria_dev/animal_project/mart/keyword_stats"
keyword_stats.write.mode("overwrite").parquet(hdfs_path_keyword)

spark.stop()