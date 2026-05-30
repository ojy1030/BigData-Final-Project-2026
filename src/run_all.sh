#!/bin/bash

echo "=========================================="
echo "파이프라인 가동 시작"

export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON='/usr/bin/python3.6'
export PYSPARK_DRIVER_PYTHON='/usr/bin/python3.6'

echo " [Step 1] 데이터 전처리(ETL) 및 HDFS 적재 시작"
spark-submit pipeline/preprocess.py

echo "[Step 2] 분석 실행: 지역별 통계 추출"
spark-submit analyze/analyze_region.py

echo "[Step 3] 분석 실행: 월별 트렌드 추출"
spark-submit analyze/analyze_monthly.py

echo "[Step 4] 분석 실행: 축종 및 품종별 통계 추출"
spark-submit analyze/analyze_breed.py

echo "=========================================="
echo " 모든 빅데이터 분석 파이프라인 완료"
