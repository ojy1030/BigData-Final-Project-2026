# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, avg, round, when

spark = SparkSession.builder.appName("AnimalAnalysis-Kind").enableHiveSupport().getOrCreate()
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

enriched_df = cleaned_df.withColumn("is_mixed", when(col("kindNm").like("%믹스%"), "Mixed").otherwise("Purebred")) \
                         .withColumn("age_group", when(col("calculated_age") <= 2, "Baby(0-2)").when((col("calculated_age") > 2) & (col("calculated_age") <= 7), "Adult(3-7)").otherwise("Senior(8+)"))

kind_stats = enriched_df.groupBy("upKindNm", "is_mixed", "age_group").agg(
    count("*").alias("total_cases"),
    round(avg("cleaned_weight"), 2).alias("avg_weight"),
    round(avg("stay_days"), 1).alias("avg_stay_days"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("euthanasia_rate"),
    round(expr("sum(case when processState like '%자연사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("natural_death_rate")
).orderBy("upKindNm", "is_mixed", "age_group")

# 🌟 Hive 테이블로 저장
kind_stats.write.mode("overwrite").saveAsTable("hive_kind_stats")
spark.stop()