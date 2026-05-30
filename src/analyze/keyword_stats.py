# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, count, avg, round

spark = SparkSession.builder.appName("AnimalAnalysis-Keyword").enableHiveSupport().getOrCreate()
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

keyword_df = cleaned_df.withColumn("detailed_trait",
    when(col("specialMark").like("%사나움%") | col("specialMark").like("%입질%") | col("specialMark").like("%경계%") | col("specialMark").like("%짖음%"), "Behavioral_Issue")
    .when(col("specialMark").like("%피부병%") | col("specialMark").like("%골절%") | col("specialMark").like("%외상%") | col("specialMark").like("%질병%"), "Medical_Issue")
    .when(col("specialMark").like("%온순%") | col("specialMark").like("%애교%") | col("specialMark").like("%친화%") | col("specialMark").like("%사람좋아%"), "Socialized")
    .otherwise("Normal_Status")
)

keyword_stats = keyword_df.groupBy("detailed_trait").agg(
    count("*").alias("total_cases"),
    round(avg("stay_days"), 1).alias("avg_stay_days"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("euthanasia_probability")
).orderBy(col("avg_stay_days").asc())

# 🌟 Hive 테이블로 저장
keyword_stats.write.mode("overwrite").saveAsTable("hive_keyword_stats")
spark.stop()