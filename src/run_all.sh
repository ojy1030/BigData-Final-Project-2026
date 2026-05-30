#!/bin/bash

echo "=========================================="
echo "파이프라인 가동 시작"

# 한글 깨짐 방지 및 파이썬3 엔진 고정
export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON='/usr/bin/python3.6'
export PYSPARK_DRIVER_PYTHON='/usr/bin/python3.6'

# 1. 공공데이터 API 수집 가동 (로컬 data/raw_animals.csv 생성)
echo " [STEP 1] 공공데이터 API 실시간 대용량 수집 가동..."
/usr/bin/python3.6 src/ingest/collect.py

# 2. 수집된 최신 CSV 파일을 HDFS 데이터 창고로 강제 업로드
echo " [STEP 2] 수집된 데이터를 HDFS 분산 파일 시스템에 적재..."
hdfs dfs -mkdir -p /user/maria_dev/animal_project/raw
hdfs dfs -put -f data/raw_animals.csv /user/maria_dev/animal_project/raw/

# 3. 하둡 전처리 파이프라인 실행
echo " [STEP 3] PySpark 기반 데이터 정제 및 결측치 처리 가동..."
spark-submit src/pipeline/preprocess.py

# 4. 4대 핵심 독립 분석 마트 생성
echo " [STEP 4] 4대 심층 분석 마트(하이브 테이블) 생성 가동..."
spark-submit src/analyze/region_stats.py
spark-submit src/analyze/kind_stats.py
spark-submit src/analyze/keyword_stats.py
spark-submit src/analyze/timeline_stats.py

echo "=========================================="
echo "SUCCESS"