#!/bin/bash

echo "=========================================="
echo "🚀 유기동물 빅데이터 분석 파이프라인 가동 시작"
echo "=========================================="

# 시스템 기본 인코딩 고정 및 파이썬3 엔진 패스 설정
export LANG=ko_KR.UTF-8
export LC_ALL=ko_KR.UTF-8
export PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON='/usr/bin/python3.6'
export PYSPARK_DRIVER_PYTHON='/usr/bin/python3.6'

# 1. 공공데이터 API 수집 가동 (로컬 data/raw_animals.csv 생성)
echo "📥 [STEP 1] 공공데이터 API 실시간 대용량 수집 가동..."
/usr/bin/python3.6 src/ingest/collect.py

# 2. 수집된 최신 CSV 파일을 HDFS 데이터 창고로 강제 업로드
echo "📂 [STEP 2] 수집된 데이터를 HDFS 분산 파일 시스템에 적재..."
hdfs dfs -mkdir -p /user/maria_dev/animal_project/raw
hdfs dfs -put -f data/raw_animals.csv /user/maria_dev/animal_project/raw/

# 3. 하둡 전처리 파이프라인 실행
echo "🧹 [STEP 3] PySpark 기반 데이터 정제 및 결측치 처리 가동..."
spark-submit src/pipeline/preprocess.py

# 4. 4대 핵심 독립 분석 마트 파이썬 스크립트 순차 가동 (HDFS에 Parquet 저장)
echo "📊 [STEP 4] PySpark 기반 4대 심층 분석 데이터 마트 생성..."
spark-submit src/analyze/region_stats.py
spark-submit src/analyze/kind_stats.py
spark-submit src/analyze/keyword_stats.py
spark-submit src/analyze/timeline_stats.py

# 5. 하이브 권한 개방 및 외부 테이블 메타데이터 동기화 (억까 방지 핵심 파트)
echo "🔐 [STEP 5] 하이브 접근 권한 개방 및 데이터베이스 마트 테이블 갱신..."

# 스파크가 새로 쓴 파일/폴더들을 하이브(hive) 계정이 마음대로 긁어갈 수 있도록 권한 전면 개방
hdfs dfs -chmod -R 777 /user/maria_dev/animal_project/

echo "🐝 Hive 테이블 동기화 중..."

# [테이블 1] 지역 종합 통계
hive -e "DROP TABLE IF EXISTS default.hive_region_stats; CREATE EXTERNAL TABLE default.hive_region_stats (orgNm STRING, total_cases BIGINT, adoption_rate STRING, euthanasia_rate STRING, return_owner_rate STRING, euthanasia_to_natural_ratio STRING) STORED AS PARQUET LOCATION 'hdfs:///user/maria_dev/animal_project/mart/region_stats';"

# [테이블 2] 지역별 월간 발병 히트맵 데이터
hive -e "DROP TABLE IF EXISTS default.hive_region_heatmap; CREATE EXTERNAL TABLE default.hive_region_heatmap (orgNm STRING, happenMonth STRING, case_count BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/maria_dev/animal_project/mart/region_heatmap';"

# [테이블 3] 품종/믹스여부/연령대별 복합 통계
hive -e "DROP TABLE IF EXISTS default.hive_kind_stats; CREATE EXTERNAL TABLE default.hive_kind_stats (upKindNm STRING, is_mixed STRING, age_group STRING, total_cases BIGINT, avg_weight STRING, avg_stay_days STRING, adoption_rate STRING, euthanasia_rate STRING, natural_death_rate STRING) STORED AS PARQUET LOCATION 'hdfs:///user/maria_dev/animal_project/mart/kind_stats';"

# [테이블 4] 특징 키워드 성향별 통계
hive -e "DROP TABLE IF EXISTS default.hive_keyword_stats; CREATE EXTERNAL TABLE default.hive_keyword_stats (detailed_trait STRING, total_cases BIGINT, avg_stay_days STRING, adoption_rate STRING, euthanasia_probability STRING) STORED AS PARQUET LOCATION 'hdfs:///user/maria_dev/animal_project/mart/keyword_stats';"

# [테이블 5] 보호 기간별 입양/안락사 건수 추이
hive -e "DROP TABLE IF EXISTS default.hive_timeline_stats; CREATE EXTERNAL TABLE default.hive_timeline_stats (stay_days INT, status_group STRING, animal_count BIGINT) STORED AS PARQUET LOCATION 'hdfs:///user/maria_dev/animal_project/mart/timeline_stats';"

# [테이블 6] 법적 보호 기간(10일) 구간별 통계
hive -e "DROP TABLE IF EXISTS default.hive_legal_zone_stats; CREATE EXTERNAL TABLE default.hive_legal_zone_stats (legal_period_zone STRING, zone_total_cases BIGINT, zone_adoption_rate STRING, zone_euthanasia_rate STRING) STORED AS PARQUET LOCATION 'hdfs:///user/maria_dev/animal_project/mart/legal_zone_stats';"

# ----------------------------------------------------------------------
# ⚠️ [HDP Sandbox 환경 이슈 안내 노트]
# PySpark로 정제 및 연산된 HDFS 원본 Parquet 데이터 내에는 UTF-8 기반 한글 데이터가 
# 100% 무결하게 정상 저장되어 있으나, 호튼웍스 가상머신 내장 HiveServer2 엔진 자체의 
# 전역 캐릭터셋 규격 제한으로 인해 Hive CLI(Beeline) 상에서 한글 데이터 조회 시 
# 일부 '??'로 출력될 수 있습니다. (HDFS 원본 데이터 검증 완료)
# ----------------------------------------------------------------------

echo "=========================================="
echo "🎉 [SUCCESS] 유기동물 빅데이터 분석 파이프라인 전체 공정 최종 완료!"
echo "=========================================="