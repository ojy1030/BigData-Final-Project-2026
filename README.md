# BigData-Final-Project-2026

1. 프로젝트 개요 
매년 급증하는 전국 유기동물 데이터를 통합 모니터링하기 위해 오픈 API 수집부터 분산 저장, 가공, DW 적재까지 전 과정을 자동화한 Scalable End-to-End 데이터 파이프라인. 
기술 스택 
Ingestion: Python 3.6 / Requests
Storage: Hadoop HDFS
Processing: Apache Spark 
Data Warehouse: Apache Hive (External Table Mapping)
Automation: Bash Shell Script (run_all.sh)
Visualization: Pandas / Matplotlib / Seaborn


2. 디렉토리 구조 (Directory)Plaintext├── 
data/
│   └── README.md                    # 데이터 출처
└── src/
    ├── run_all.sh                   # 파이프라인 통합 제어 스크립트
    ├── ingest/
    │   └── collect.py               # API 데이터 수집 엔진
    ├── pipeline/
    │   └── preprocess.py            # PySpark 데이터 정제 및 전처리
    └── analyze/
        ├── eda.py                   # 지표 유효성 검증 레포트 스크립트
        ├── region_stats.py          # 지리적 공간 편중성 마트 연산 코어
        ├── kind_stats.py            # 축종/생애주기 복합 교차 집계 코어
        ├── keyword_stats.py         # 비정형 특이사항 텍스트 마이닝 코어
        ├── timeline_stats.py        # 보호 기간 타임라인 구간화 비닝 코어
        └── visualize.py             # HDFS 마트 스트림 수확 및 시각화 엔진   

3. 실행 방법 (How to Run)
   3.1 터미널 인코딩 설정 
   Bashexport LANG=en_US.UTF-8
   export LC_ALL=en_US.UTF-8

   3.2 종속성 패키지 설치 & 파이프라인 일괄 가동
      1. 필수 패키지 설치
      /usr/bin/python3.6 -m pip install --user requests pandas matplotlib seaborn

      2. src 폴더로 이동 후 통합 스크립트 실행
         cd src
         chmod +x run_all.sh
         ./run_all.sh

4. 핵심 결과 요약  
   1. 공간적 편중성 현상: 
   제주(10,221건)와 경기도 대구에 유기동물 발생이 집중됨.  
   2. 시간적 포화도 현상: 
   구조 개체의 62.9%가 입소 후 10일 이내에 처분 종료됨.  
   분석: 20일 초과 장기 체류는 단 0.1%로 보호소 수용 공간이 이미 포화 상태임을 입증.  
   3. 비정형 성향 위험도 현상:
    공격성 개체의 안락사율은 48.20%로 가장 높음.  
    분석: 온순한 개체도 22.26% 안락사되며 보호소 포화 시 성격과 무관하게 처분되는 한계 확인.  
   4. 개체 복합 연령 분석 현상: 
   순종(Purebred)은 믹스견보다 몸무게가 무거운 중·대형 품종 위주로 유기됨.  
   분석: 8세 이상 노령기 순종견은 몸무게 편차가 매우 크며 관리 부담으로 인한 집단 유기 성향을 보임. 
   
   최종 결론: 보호소 포화를 막고 유기동물 생존율을 높이려면 모든 예산과 행정 자원을 입소 직후 '최초 10일 이내의 골든타임'에 집중 투입해야 함.
   
5. AI 사용 내역 
   Gemini: PySpark 파이프라인 경로 오류 및 HDFS 권한 블로킹 디버깅 자문
   Gemini: Matplotlib/Seaborn 기반 영문 차트 레이아웃 가독성 확보 및 테마 컬러 추천
   Gemini: 소스 코드 연산 결과 데이터와 최종 프로젝트 보고서 본문 간의 통계 수치 정합성 교정