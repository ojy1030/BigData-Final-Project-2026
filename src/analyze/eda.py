# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, min, max, avg, round, regexp_extract, lit

spark = SparkSession.builder.appName("Animal-Putty-EDA-Final").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


print("EDA")
print("="*65)

df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

total_rows = df.count()
print("총 정제 데이터 건수: {0} 건".format(total_rows))
print("-"*65)


# 결측치 상태 평가
print("주요 핵심 컬럼별 결측치 누계 현황:")
null_cols = ["orgNm", "upKindNm", "kindNm", "processState", "sexCd", "neuterYn"]
null_stats = df.select([count(when(col(c).isNull(), c)).alias(c) for c in null_cols]).collect()[0]

print(" | ".join(null_cols))
print("-" * 50)
print(" | ".join([str(null_stats[c]) for c in null_cols]))
print("-"*65)


# 수치형 데이터 지표 평가
print("수치형 지표 요약 통계량:")

analysis_df = df.withColumn("extracted_age", regexp_extract(col("age"), r"(\d+)", 1).cast("double"))
analysis_df = analysis_df.withColumn(
    "cleaned_age",
    when((col("extracted_age") >= 1900) & (col("extracted_age") <= 2026), lit(2026) - col("extracted_age"))
    .when((col("extracted_age") > 0) & (col("extracted_age") < 100), col("extracted_age"))
    .otherwise(lit(None).cast("double")) 
)

analysis_df = analysis_df.withColumn("extracted_weight", regexp_extract(col("weight"), r"([\d.]+)", 1).cast("double"))
analysis_df = analysis_df.withColumn(
    "cleaned_weight",
    when((col("extracted_weight") >= 0.1) & (col("extracted_weight") <= 150.0), col("extracted_weight"))
    .otherwise(lit(None).cast("double")) 
)

# 정상 범주 내의 유효 수치만 뽑아서 계산
valid_df = analysis_df.filter(col("cleaned_age").isNotNull() & col("cleaned_weight").isNotNull())

stats = valid_df.select(
    round(avg("cleaned_age"), 1).alias("avg_age"),
    max("cleaned_age").alias("max_age"),
    round(avg("cleaned_weight"), 2).alias("avg_weight"),
    max("cleaned_weight").alias("max_weight")
).collect()

if stats and stats[0]["avg_age"] is not None:
    row = stats[0]
    print("평균 나이: {0}세 | 최대 나이: {1}세".format(row["avg_age"], int(row["max_age"])))
    print("평균 체중: {0}kg | 최대 체중: {1}kg".format(row["avg_weight"], row["max_weight"]))
else:
    print("유효한 범위의 나이/체중 데이터를 찾을 수 없음.")
print("-"*65)


# 축종별 분포 체크
print("축종별 유기동물 분포 현황:")
kind_rows = df.groupBy("upKindNm").count().orderBy(col("count").desc()).collect()
for row in kind_rows:
    kind_name = row["upKindNm"] if row["upKindNm"] is not None else "Unknown"
    if not isinstance(kind_name, str):
        kind_name = str(kind_name)
    print("- {0}: {1} 건".format(kind_name, row["count"]))
print("-"*65)


print("최종 처분 상태 분포 상위 5개:")

# 'M', 'F' 등 처분 상태에 성별이 잘못 들어온 행을 'Unknown'으로 치환
clean_state_df = df.withColumn(
    "fixed_processState",
    when(col("processState").startswith(u"종료") | col("processState").contains(u"보호"), col("processState"))
    .otherwise(lit("Unknown"))
)

state_rows = clean_state_df.groupBy("fixed_processState").count().orderBy(col("count").desc()).limit(5).collect()
for row in state_rows:
    state_name = row["fixed_processState"] if row["fixed_processState"] is not None else "Unknown"
    if not isinstance(state_name, str):
        state_name = str(state_name)
    print("- {0}: {1} 건".format(state_name, row["count"]))
print("="*65 + "\n")

spark.stop()
#3