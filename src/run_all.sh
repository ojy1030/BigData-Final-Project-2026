#!/bin/bash

echo "유기동물 빅데이터 분석 파이프라인 가동"
echo "=================================================================="

# 🌟 [경로 방어벽 완벽 튜닝] 스크립트가 src/ 안에 있더라도, 무조건 프로젝트 최상위 루트로 디렉토리를 정확하게 고정합니다.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [[ "$SCRIPT_DIR" == */src ]]; then
    cd "$SCRIPT_DIR/.."
else
    cd "$SCRIPT_DIR"
fi

# 시스템 기본 인코딩 고정 및 파이썬3 엔진 패스 설정
export LANG=ko_KR.UTF-8
export LC_ALL=ko_KR.UTF-8
export PYTHONIOENCODING=utf-8
export PYSPARK_PYTHON='/usr/bin/python3.6'
export PYSPARK_DRIVER_PYTHON='/usr/bin/python3.6'

# 공공데이터 API 수집 가동 
echo "공공데이터 API 실시간 수집 가동"
/usr/bin/python3.6 src/ingest/collect.py

echo "하둡 HDFS 권한"
sudo -u hdfs hdfs dfs -mkdir -p /user/maria_dev/animal_project/mart
sudo -u hdfs hdfs dfs -chown -R maria_dev:hdfs /user/maria_dev/animal_project/mart
sudo -u hdfs hdfs dfs -chmod -R 777 /user/maria_dev/animal_project/

# 수집된 CSV 파일을 HDFS 데이터 업로드
echo "수집된 raw 데이터를 HDFS 분산 파일 시스템에 적재"
hdfs dfs -mkdir -p /user/maria_dev/animal_project/raw
hdfs dfs -put -f data/raw_animals.csv /user/maria_dev/animal_project/raw/

# 하둡 전처리 파이프라인 실행
echo "PySpark 기반 데이터 정제 및 결측치 처리 가동"
spark-submit src/pipeline/preprocess.py

echo "EDA 가동"
PYTHONIOENCODING=utf-8 PYSPARK_PYTHON=/usr/bin/python3.6 PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6 spark-submit src/analyze/eda.py

# 파이썬 분석 스크립트 순차 가동 
echo "PySpark 기반 핵심 데이터 마트 생성 엔진 기동"
spark-submit src/analyze/region_stats.py
spark-submit src/analyze/kind_stats.py
spark-submit src/analyze/keyword_stats.py
spark-submit src/analyze/timeline_stats.py  


echo "하이브 전용 최종 접근 권한 갱신"
sudo -u hdfs hdfs dfs -chmod -R 777 /user/maria_dev/animal_project/

echo "Hive DW 외부 테이블 스키마 매핑"
hive -e "DROP TABLE IF EXISTS default.hive_region_stats; CREATE EXTERNAL TABLE default.hive_region_stats (orgnm STRING, total_cases BIGINT, adoption_rate STRING, euthanasia_rate STRING, return_owner_rate STRING, euthanasia_to_natural_ratio STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs:///user/maria_dev/animal_project/mart/region_stats';"
hive -e "DROP TABLE IF EXISTS default.hive_kind_stats; CREATE EXTERNAL TABLE default.hive_kind_stats (upkindnm STRING, is_mixed STRING, age_group STRING, total_cases BIGINT, avg_weight STRING, avg_stay_days STRING, adoption_rate STRING, euthanasia_rate STRING, natural_death_rate STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs:///user/maria_dev/animal_project/mart/kind_stats';"
hive -e "DROP TABLE IF EXISTS default.hive_keyword_stats; CREATE EXTERNAL TABLE default.hive_keyword_stats (detailed_trait STRING, total_cases BIGINT, avg_stay_days DOUBLE, adoption_rate DOUBLE, euthanasia_probability DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs:///user/maria_dev/animal_project/mart/keyword_stats';"
hive -e "DROP TABLE IF EXISTS default.hive_legal_zone_stats; CREATE EXTERNAL TABLE default.hive_legal_zone_stats (legal_period_zone STRING, zone_total_cases BIGINT, zone_adoption_rate DOUBLE, zone_euthanasia_rate DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs:///user/maria_dev/animal_project/mart/legal_zone_stats';"

echo "암바리 UI 동기화용 데이터 마트 소유권 강제 양도"
sudo -u hdfs hdfs dfs -chown -R maria_dev:hdfs /user/maria_dev/animal_project/mart/

echo "데이터 시각화"
if [ -f "src/analyze/visualize.py" ]; then
    /usr/bin/python3.6 src/analyze/visualize.py
    
    echo "생성된 차트 이미지를 하둡 mart 폴더로 업로드 중..."
    hdfs dfs -put -f chart_*.png /user/maria_dev/animal_project/mart
    
    # 3. 암바리 웹 UI에서 파일이 즉시 보이고 다운로드 가능하도록 권한 개방
    hdfs dfs -chmod 777 /user/maria_dev/animal_project/mart/chart_*.png
    
    echo "시각화 완료 및 암바리 UI 동기화 대성공"
else
    echo "visualize.py 파일을 찾을 수 없음."
fi

echo "=================================================================="
echo "SUCCESS"
#