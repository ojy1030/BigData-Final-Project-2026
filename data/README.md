# Data Directory Overview

1. 데이터 출처 (Data Source)
제공 기관: 농림축산식품부 농림축산검역본부 (국가동물보호정보시스템)

데이터셋명: 유기동물 정보 조회 서비스 Open API

수집 대상: 2024년 ~ 2026년 전국 유기동물 공고 데이터 전체 

참고 사항: run_all.sh 가동 시 자동 수집되어 HDFS에 적재됩니다.

2. 원본 데이터 스키마
오픈 API를 통해 수집되는 원천 데이터의 22개 전체 필드 명세입니다.

desertionNo: 유기번호 

happenDt: 발견일자 (YYYYMMDD)

happenPlace: 발견장소

kindNm: 품종 정보 (예: [개] 믹스견)

colorNm: 모색 (털 색상)

age: 나이 (예: 2024(년생))

weight: 체중 (예: 3.5(Kg))

sexCd: 성별 (M: 남, F: 여, Q: 미상)

neuterYn: 중성화 여부 (Y: 전환, N: 미전환, U: 미상)

specialMark: 특징 및 특이사항 (비정형 텍스트)

noticeNo: 공고번호

noticeSdt: 공고시작일 (YYYYMMDD)

noticeEdt: 공고종료일 (YYYYMMDD)

processState: 최종 보호상태 (입양, 안락사, 자연사 등)

careNm: 관할 보호소명

careTel: 보호소 전화번호

careAddr: 보호소 주소

orgNm: 관할 지자체명

chargeNm: 담당자 이름

officetel: 담당자 연락처

filename: 썸네일 이미지 경로

popfile: 원본 이미지 경로