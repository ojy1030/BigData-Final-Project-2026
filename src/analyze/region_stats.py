# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, count, round

# 샌드박스 하이브 메타스토어 주소 강제 연동 세팅 추가
spark = SparkSession.builder \
    .appName("AnimalAnalysis-Region") \
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 정제된 데이터 로드
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

# [분석 1] 지역 종합 통계
region_stats = cleaned_df.groupBy("orgNm").agg(
    count("*").alias("total_cases"),
    round(expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("adoption_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("euthanasia_rate"),
    round(expr("sum(case when processState like '%반환%' then 1 else 0 end) * 100.0 / count(*)"), 2).alias("return_owner_rate"),
    round(expr("sum(case when processState like '%안락사%' then 1 else 0 end) / nullif(sum(case when processState like '%자연사%' then 1 else 0 end), 0)"), 2).alias("euthanasia_to_natural_ratio")
).orderBy(col("total_cases").desc())

# 🌟 [분석 1 저장] HDFS에 Parquet 형식으로 물리 저장 후 Hive 메타스토어 강제 등록
hdfs_path_stats = "hdfs:///user/maria_dev/animal_project/mart/region_stats"
region_stats.write.mode("overwrite").parquet(hdfs_path_stats)

spark.sql("DROP TABLE IF EXISTS default.hive_region_stats")
spark.sql(f"""
    CREATE EXTERNAL TABLE default.hive_region_stats (
        orgNm STRING,
        total_cases BIGINT,
        adoption_rate DOUBLE,
        euthanasia_rate DOUBLE,
        return_owner_rate DOUBLE,
        euthanasia_to_natural_ratio DOUBLE
    )
    STORED AS PARQUET
    LOCATION '{hdfs_path_stats}'
""")

# [분석 2] 히트맵용 데이터
top_15 = [row['orgNm'] for row in region_stats.limit(15).select("orgNm").collect()]
heatmap_data = cleaned_df.filter(col("orgNm").isin(top_15)).groupBy("orgNm", "happenMonth").agg(count("*").alias("case_count"))

# 🌟 [분석 2 저장] HDFS에 Parquet 형식으로 물리 저장 후 Hive 메타스토어 강제 등록
hdfs_path_heatmap = "hdfs:///user/maria_dev/animal_project/mart/region_heatmap"
heatmap_data.write.mode("overwrite").parquet(hdfs_path_heatmap)

spark.sql("DROP TABLE IF EXISTS default.hive_region_heatmap")
spark.sql(f"""
    CREATE EXTERNAL TABLE default.hive_region_heatmap (
        orgNm STRING,
        happenMonth STRING,
        case_count BIGINT
    )
    STORED AS PARQUET
    LOCATION '{hdfs_path_heatmap}'
""")

spark.stop()