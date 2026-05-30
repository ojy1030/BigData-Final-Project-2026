from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, avg, round

spark = SparkSession.builder.appName("AnimalAnalysis-Kind").getOrCreate()
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

kind_stats = cleaned_df.groupBy("upKindNm").agg(
    count("*").alias("total_cases"),
    round(avg("cleaned_weight"), 2).alias("avg_weight"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("euthanasia_rate"),
    round(expr("sum(case when processState like '%자연사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("natural_death_rate")
).orderBy(col("total_cases").desc())

kind_stats.write.mode("overwrite").parquet("hdfs:///user/maria_dev/animal_project/output/kind_stats")
spark.stop()