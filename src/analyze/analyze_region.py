from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr

# 1. 스파크 세션 시작
spark = SparkSession.builder.appName("AnalyzeRegion").getOrCreate()

print("⏳ 1. 하둡에서 전처리된 데이터를 읽어오는 중...")
cleaned_df = spark.read.parquet("hdfs:///user/maria_dev/animal_project/processed/")

print("📊 2. 지역별 유기동물 입양률 통계 연산 중 (Spark Cluster)...")
adoption_stats = cleaned_df.groupBy("orgNm").agg(
    count("*").alias("total_cases"),
    # 소수점 보장을 위해 100.0을 곱해줌
    expr("sum(case when processState like '%입양%' then 1 else 0 end) * 100.0 / count(*)").alias("adoption_rate")
).orderBy(col("adoption_rate").desc())

# HDFS 창고에 요약본 저장
adoption_stats.write.mode("overwrite").parquet("hdfs:///user/maria_dev/data/report_region")

print("=== [RESULT 1] REGION ADOPTION STATS (Top 20) ===")
adoption_stats.show(20, truncate=False)



print("📈 3. 시각화를 위해 상위 20개 요약 데이터를 수집 중...")

# 수십만 건이 아니라 딱 상위 20줄만 로컬 단으로 가져오는 거라 서버 메모리에 전혀 부담 없음!
top_20_pandas = adoption_stats.limit(20).toPandas()

print("🎨 4. Plotly Express를 활용해 인터랙티브 그래프 생성 중...")
import plotly.express as px

# 주영이가 올려준 Plotly 강의자료 1-2절 express 활용 응용
fig = px.bar(
    top_20_pandas, 
    x="adoption_rate", 
    y="orgNm", 
    orientation="h",  # 가로 막대 그래프
    title="Top 20 지역별 유기동물 입양률 (%) 통계",
    labels={"adoption_rate": "입양률 (%)", "orgNm": "관할 구역"},
    color="adoption_rate",
    color_continuous_scale="Blues"
)

# 레이아웃 정돈 (글자 잘림 방지 및 Y축 역순 정렬로 1등이 맨 위로 오게 설정)
fig.update_layout(
    yaxis={'categoryorder':'total ascending'},
    title_font_size=20,
    margin=dict(l=150, r=20, t=60, b=40)
)

# 🔥 핵심 치트키: plt.show() 대신 HTML 파일로 서버에 직접 굽기!
output_filename = "region_adoption_graph.html"
fig.write_html(output_filename)

print(f"그래프가 '{output_filename}' 파일로 현재 폴더에 저장되었습니다!")

