import csv
import os
import requests
import pandas as pd
import time

# True : 딱 1페이지만 빠르게 수집
# False: 전체 데이터 수집

IS_GRADING_MODE = False
# IS_GRADING_MODE = True


DECODING_KEY = "HpJGL7BvOA3rXvZz45iwccKnjBch2u4h7FrglCEn4xEsWj1Pm/ILD9PLQlg4ark72DzBEJxhzFWT9fo1sJ65mw=="
URL = "https://apis.data.go.kr/1543061/abandonmentPublicService_v2/abandonmentPublic_v2"

os.makedirs("data", exist_ok=True)
OUTPUT_FILE = "data/raw_animals.csv"


if IS_GRADING_MODE:
    print("최소 데이터만 수집")
    TARGET_YEARS = ["2025"] 
    MAX_PAGES = 1
    NUM_ROWS = 10  # 테스트용 10건만 호출
else:
    print("전체 데이터 수집")
    TARGET_YEARS = ["2023", "2024", "2025"]  
    MAX_PAGES = 100 
    NUM_ROWS = 1000

all_data = []

for year in TARGET_YEARS:
    print(f" {year}년도 데이터 수집 시작")
    
    page = 1
    while page <= MAX_PAGES:
        params = {
            "serviceKey": DECODING_KEY,
            "bgnde": f"{year}0101",
            "endde": f"{year}1231",
            "pageNo": str(page),
            "numOfRows": str(NUM_ROWS),
            "_type": "json"
        }
        
        try:
            response = requests.get(URL, params=params, timeout=20)
            
            if response.status_code == 200:
                res_json = response.json()
                body = res_json.get("response", {}).get("body", {})
                items = body.get("items", {})
                
                if items and "item" in items:
                    item_list = items["item"]
                    
                    if isinstance(item_list, dict):
                        item_list = [item_list]
                        
                    all_data.extend(item_list)
                    print(f"   Page {page}: {len(item_list)}건 수집 완료 (누적: {len(all_data)}건)")
                    
                    if len(item_list) < NUM_ROWS:
                        break
                else:
                    print(f"   Page {page}: 더 이상 데이터가 없음")
                    break
            else:
                print(f"   Page {page}: API 응답 에러 (코드: {response.status_code})")
                break
                
            page += 1
            time.sleep(1.5)
            
        except requests.exceptions.Timeout:
            print(f" ⚠️ Page {page}: API 응답 시간 초과")
            time.sleep(5)
            continue  
            
        except Exception as e:
            print(f"   Page {page}: 예외 발생 ({str(e)})")
            break

# 수집된 데이터를 DataFrame으로 묶어서 저장
if all_data:
    final_df = pd.DataFrame(all_data)
    
    if "desertionNo" in final_df.columns:
        final_df.drop_duplicates(subset=["desertionNo"], inplace=True)
        
    final_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\n 파일 저장 완료: {OUTPUT_FILE}")
    print(f" 총 수집 건수: {len(final_df):,} 건")
    print(f" 파일 용량: {file_size_mb:.2f} MB")
    
    if IS_GRADING_MODE:
        print(" 수집 기능 정상 작동")
    else:
        if file_size_mb >= 100:
            print(" 100MB 이상 충족 완료")
        else:
            print(" 용량 부족")
else:
    print(" 수집된 데이터 없음")