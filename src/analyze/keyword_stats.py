# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, count, avg, round, to_date, datediff

# 스파크 세션 생성
spark = SparkSession.builder.appName("AnimalAnalysis-Keyword-Fixed").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 정제된 데이터 로드
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

analysis_df = cleaned_df.withColumn("start_date", to_date(col("noticeSdt"), "yyyyMMdd")) \
                        .withColumn("end_date", to_date(col("noticeEdt"), "yyyyMMdd")) \
                        .withColumn(
                            "stay_days",
                            when((datediff(col("end_date"), col("start_date")) >= 0) & (datediff(col("end_date"), col("start_date")) <= 365), datediff(col("end_date"), col("start_date")))
                            .otherwise(10)
                        )

# 특징 기반 핵심 키워드 분류 파생 변수 생성
keyword_df = analysis_df.withColumn("detailed_trait",
    when(col("specialMark").like("%사나움%") | col("specialMark").like("%입질%") | col("specialMark").like("%경계%") | col("specialMark").like("%짖음%"), "Behavioral_Issue")
    .when(col("specialMark").like("%피부병%") | col("specialMark").like("%골절%") | col("specialMark").like("%외상%") | col("specialMark").like("%질병%"), "Medical_Issue")
    .when(col("specialMark").like("%온순%") | col("specialMark").like("%애교%") | col("specialMark").like("%친화%") | col("specialMark").like("%사람좋아%"), "Socialized")
    .otherwise("Normal_Status")
)

# 유기동물 성향 키워드별 종합 통계 계산
keyword_stats = keyword_df.groupBy("detailed_trait").agg(
    count("*").alias("total_cases"),
    round(avg("stay_days"), 1).cast("string").alias("avg_stay_days"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).cast("string").alias("euthanasia_probability")
).orderBy("avg_stay_days")

# 저장
hdfs_path_keyword = "hdfs:///user/maria_dev/animal_project/mart/keyword_stats"
keyword_stats.write.mode("overwrite").option("header", "false").csv(hdfs_path_keyword)

spark.stop()
#