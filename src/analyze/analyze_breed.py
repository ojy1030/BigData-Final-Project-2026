from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr

# 1. 스파크 세션 시작
spark = SparkSession.builder.appName("AnalyzeBreed").getOrCreate()

print("⏳ 1. 하둡에서 전처리된 데이터를 읽어오는 중...")
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

print("📊 2. 품종별 통계 및 입양률 연산 중 (Spark Cluster)...")
breed_stats = cleaned_df.groupBy("kindCd").agg(
    count("*").alias("total_cases"),
    # 소수점 정밀도를 위해 100.0으로 수정
    expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)").alias("adoption_rate")
).filter(col("total_cases") > 100).orderBy(col("adoption_rate").desc())

print("=== [RESULT 3] BREED ADOPTION STATS ===")
breed_stats.show(20, truncate=False)

# 파이프라인 연산 결과를 HDFS 창고에도 백업 저장
breed_stats.write.mode("overwrite").parquet("hdfs:///user/maria_dev/data/report_breed")


# ==========================================================
# 🚀 [시각화 단계] 산점도(Scatter Plot) 그래프 파일 생성
# ==========================================================
print("📈 3. 시각화를 위해 품종 통계 데이터를 수집 중...")
# 100건 이상인 품종들만 모인 거라 로컬 Pandas로 가져와도 용량이 매우 가벼움!
breed_pandas = breed_stats.toPandas()

print("🎨 4. Plotly를 활용해 품종별 다차원 산점도 그래프 생성 중...")
import plotly.express as px

# 아까 막대(Bar)를 썼으니, 이번엔 총 건수(X)와 입양률(Y)을 동시에 매핑하는 Scatter 차트 사용
fig = px.scatter(
    breed_pandas,
    x="total_cases",       # X축: 유기동물 발생 건수
    y="adoption_rate",     # Y축: 입양률 (%)
    hover_name="kindCd",   # 마우스를 점에 올리면 품종 이름이 팝업으로 뜨게 설정
    size="total_cases",    # 많이 발생하는 품종일수록 점의 크기가 커짐 (직관성 업)
    color="adoption_rate", # 입양률이 높을수록 진한 색상으로 표시
    title="품종별 유기동물 발생 건수 대비 입양률 분석 (100건 이상 메이저 품종)",
    labels={"total_cases": "총 발생 건수 (마리)", "adoption_rate": "입양률 (%)"},
    color_continuous_scale="Viridis"
)

# 레이아웃 글자 크기 조정
fig.update_layout(
    title_font_size=20,
    margin=dict(l=50, r=50, t=70, b=50)
)

# 🔥 핵심 치트키: HTML 파일로 현재 폴더에 구워버리기
output_filename = "breed_adoption_graph.html"
fig.write_html(output_filename)

print(f"🎉 [성공] 품종별 산점도 그래프가 '{output_filename}' 파일로 저장되었습니다!")

spark.stop()