from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

spark = SparkSession.builder.appName("AnimalAnalysis-Timeline").getOrCreate()
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

timeline_stats = cleaned_df.filter(col("processState").like("%입양%") | col("processState").like("%안락사%")) \
    .withColumn("status_group", when(col("processState").like("%입양%"), "Adoption").otherwise("Euthanasia")) \
    .groupBy("stay_days", "status_group").agg(
        count("*").alias("animal_count")
    ).orderBy("stay_days")

timeline_stats.write.mode("overwrite").parquet("hdfs:///user/maria_dev/animal_project/output/timeline_stats")
spark.stop()