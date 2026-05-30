from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, round

spark = SparkSession.builder.appName("AnimalAnalysis-Region").getOrCreate()
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

# 1. 지역별 총괄 (산점도용: 발생량 vs 입양률)
region_stats = cleaned_df.groupBy("orgNm").agg(
    count("*").alias("total_cases"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("euthanasia_rate")
).orderBy(col("total_cases").desc())

region_stats.write.mode("overwrite").parquet("hdfs:///user/maria_dev/animal_project/output/region_stats")

# 2. 월별 발생 밀도 (히트맵용: 상위 15개 지역)
top_15 = [row['orgNm'] for row in region_stats.limit(15).select("orgNm").collect()]
heatmap_data = cleaned_df.filter(col("orgNm").isin(top_15)) \
    .groupBy("orgNm", "happenMonth") \
    .agg(count("*").alias("case_count"))

heatmap_data.write.mode("overwrite").parquet("hdfs:///user/maria_dev/animal_project/output/region_heatmap")
spark.stop()