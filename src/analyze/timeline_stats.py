# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, round, expr

spark = SparkSession.builder.appName("AnimalAnalysis-Timeline").enableHiveSupport().getOrCreate()
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

timeline_stats = cleaned_df.filter(col("processState").like("%입양%") | col("processState").like("%안락사%")) \
    .withColumn("status_group", when(col("processState").like("%입양%"), "Adoption").otherwise("Euthanasia")) \
    .groupBy("stay_days", "status_group").agg(count("*").alias("animal_count")).orderBy("stay_days", "status_group")

# 🌟 Hive 테이블로 저장
timeline_stats.write.mode("overwrite").saveAsTable("hive_timeline_stats")

legal_zone_stats = cleaned_df.withColumn("legal_period_zone",
    when(col("stay_days") <= 10, "01_Within_Legal_Period(0-10d)").when((col("stay_days") > 10) & (col("stay_days") <= 20), "02_Critical_Zone(11-20d)").otherwise("03_Long_Term_Overdue(>20d)")
).groupBy("legal_period_zone").agg(
    count("*").alias("zone_total_cases"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("zone_adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("zone_euthanasia_rate")
).orderBy("legal_period_zone")

# 🌟 Hive 테이블로 저장
legal_zone_stats.write.mode("overwrite").saveAsTable("hive_legal_zone_stats")
spark.stop()