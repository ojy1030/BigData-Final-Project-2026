from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, count, avg, round

spark = SparkSession.builder.appName("AnimalAnalysis-Keyword").getOrCreate()
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

keyword_stats = cleaned_df.withColumn("trait_group", 
    when(col("specialMark").like("%온순%") | col("specialMark").like("%사람좋아%") | col("specialMark").like("%애교%"), "Positive")
    .when(col("specialMark").like("%사나움%") | col("specialMark").like("%입질%") | col("specialMark").like("%겁%") | col("specialMark").like("%피부병%"), "Negative")
    .otherwise("Normal")
).groupBy("trait_group").agg(
    count("*").alias("total_cases"),
    round(avg("stay_days"), 1).alias("avg_stay_days"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("adoption_rate")
).orderBy(col("avg_stay_days").asc())

keyword_stats.write.mode("overwrite").parquet("hdfs:///user/maria_dev/animal_project/output/keyword_stats")
spark.stop()