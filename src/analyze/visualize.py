# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import seaborn as sns
import subprocess
import io

print("=== START: High-Quality English Data Visualization (Fixed) ===")

plt.rcdefaults()
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['axes.unicode_minus'] = False

sns.set_theme(style="whitegrid")
sns.set_context("talk")  

sido_eng_map = {
    '경기': 'Gyeonggi', '서울': 'Seoul', '인천': 'Incheon', '부산': 'Busan', '대구': 'Daegu',
    '광주': 'Gwangju', '대전': 'Daejeon', '울산': 'Ulsan', '세종': 'Sejong', '강원': 'Gangwon',
    '충북': 'Chungbuk', '충남': 'Chungnam', '전북': 'Jeonbuk', '전남': 'Jeonnam', '경북': 'Gyeongbuk', '경남': 'Gyeongnam', '제주': 'Jeju'
}

try:
    print("Rendering region_stats (Top 4 Region Layout)")
    cmd = "hdfs dfs -cat /user/maria_dev/animal_project/mart/region_stats/part-*"
    data = subprocess.check_output(cmd, shell=True)
    df_region = pd.read_csv(io.BytesIO(data), names=["sido", "sigungu", "total_cases", "adopt_rate", "euthanasia_rate", "return_rate", "ratio"])
    
    # 지자체별 그룹화 후 상위 10개 추출 
    df_top_region = df_region.groupby("sido").agg({"total_cases": "sum"}).reset_index()
    df_top_region = df_top_region.sort_values(by="total_cases", ascending=False).head(10)
    
    def force_english_zone(x):
        val = str(x).strip()
        if '제주' in val: return 'Jeju'
        elif '대구' in val: return 'Daegu'
        elif '경기' in val: return 'Gyeonggi'
        return 'Other Region'

    df_top_region['Location'] = df_top_region['sido'].apply(force_english_zone)
    df_top_region = df_top_region.sort_values(by="total_cases", ascending=False)
    
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.barplot(x="total_cases", y="Location", data=df_top_region, palette="Blues_r", ax=ax)
    
    ax.set_title("Top 4 Regions by Abandoned Animal Cases", fontsize=16, fontweight='bold', pad=15)
    ax.set_xlabel("Total Cases (Count)", fontsize=13)
    ax.set_ylabel("Geographic Location", fontsize=13)
    
    plt.subplots_adjust(left=0.28, right=0.95, top=0.9, bottom=0.12)
    
    plt.savefig("chart_region_total_hbar.png", dpi=300)
    plt.close()
    print("-> SUCCESS")
except Exception as e:
    print("-> ERROR", e)
    
# kind_stats 연령대 및 품종별 평균 몸무게 비교 
try:
    print("Rendering kind_stats")
    cmd = "hdfs dfs -cat /user/maria_dev/animal_project/mart/kind_stats/part-*"
    data = subprocess.check_output(cmd, shell=True)
    df_kind = pd.read_csv(io.BytesIO(data), names=["upkindnm", "is_mixed", "age_group", "total", "weight", "avg_stay", "adopt_rate", "euthanasia_rate", "natural_death_rate"])
    
    df_kind = df_kind.dropna(subset=["age_group", "weight"])
    df_kind['Breed Type'] = df_kind['is_mixed'].apply(lambda x: 'Mixed Breed' if str(x).strip() == 'Mixed' else 'Purebred')
    
    age_map = {
        'Baby(0-2)': 'Baby (0-2 yo)', 
        'Adult(3-7)': 'Adult (3-7 yo)', 
        'Senior(8+)': 'Senior (8+ yo)'
    }
    df_kind['Age Group'] = df_kind['age_group'].str.strip().map(age_map).fillna(df_kind['age_group'])
    
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.barplot(x="Age Group", y="weight", hue="Breed Type", data=df_kind, palette="Set2", ax=ax)
    
    ax.set_title("Average Weight by Age Group & Breed Type", fontsize=16, fontweight='bold', pad=15)
    ax.set_xlabel("Age Classification", fontsize=13)
    ax.set_ylabel("Average Weight (kg)", fontsize=13)
    plt.legend(title="Breed Classification", loc="upper left")
    
    plt.tight_layout()
    plt.savefig("chart_kind_weight_bar.png", dpi=300)
    plt.close()
    print("-> SUCCESS")
except Exception as e:
    print("-> ERROR", e)

# keyword_stats 성향별 안락사 위험도 차트
try:
    print("Rendering keyword_stats")
    cmd = "hdfs dfs -cat /user/maria_dev/animal_project/mart/keyword_stats/part-*"
    data = subprocess.check_output(cmd, shell=True)
    df_key = pd.read_csv(io.BytesIO(data), names=["trait", "total", "avg_stay", "adopt_rate", "euthanasia_prob"])
    
    df_key = df_key.dropna(subset=["trait", "euthanasia_prob"])
    
    trait_eng_map = {
        'Normal_Status': 'Healthy / Normal',
        'Socialized': 'Friendly / Socialized',
        'Medical_Issue': 'Illness / Injured',
        'Behavioral_Issue': 'Behavioral Issue / Aggressive'
    }
    df_key['Animal Trait'] = df_key['trait'].astype(str).str.strip().apply(lambda x: trait_eng_map.get(x, x))
    df_key_sorted = df_key.sort_values(by="euthanasia_prob", ascending=False)
    
    fig, ax = plt.subplots(figsize=(13, 7))
    sns.barplot(x="euthanasia_prob", y="Animal Trait", data=df_key_sorted, palette="Reds_r", ax=ax)
    
    ax.set_title("Euthanasia Probability by Animal Traits", fontsize=16, fontweight='bold', pad=15)
    ax.set_xlabel("Euthanasia Risk Probability (%)", fontsize=13)
    ax.set_ylabel("Extracted Trait Keywords", fontsize=13)
    
    # 🌟 행동 성향 라벨 역시 길기 때문에 왼쪽 마진을 넉넉하게 고정 배정
    plt.subplots_adjust(left=0.32, right=0.95, top=0.9, bottom=0.12)
    plt.savefig("chart_keyword_euthanasia_hbar.png", dpi=300)
    plt.close()
    print("-> SUCCESS")
except Exception as e:
    print("-> ERROR", e)

# timeline_stats 보호 기간 구간별 도넛 파이 차트 
try:
    print("Rendering timeline_stats")
    cmd = "hdfs dfs -cat /user/maria_dev/animal_project/mart/legal_zone_stats/part-*"
    data = subprocess.check_output(cmd, shell=True)
    df_legal = pd.read_csv(io.BytesIO(data), names=["zone", "total_cases", "adoption_rate", "euthanasia_rate"])
    
    sizes = df_legal['total_cases'].dropna().tolist()
    zones = df_legal['zone'].dropna().tolist()
    
    labels = []
    for z in zones:
        z_str = str(z).strip()
        if '01' in z_str or '0_10' in z_str or 'Legal' in z_str:
            labels.append('Within Legal Notice Period\n(0-10 Days Protective)')
        elif '02' in z_str or '11_20' in z_str or 'Critical' in z_str:
            labels.append('Intensive Adoption Period\n(11-20 Days Protective)')
        else:
            labels.append('Long-term Overdue Period\n(>20 Days Protective)')
        
    colors = ['#5b9bd5', '#ed7d31', '#70ad47', '#ffc000'][:len(sizes)]
    
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors,
        pctdistance=0.7, wedgeprops=dict(width=0.4, edgecolor='w'), textprops={'fontsize': 12, 'fontweight': 'bold'})
    
    ax.set_title("Distribution of Cases by Protection Period Zones", fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig("chart_legal_zone_pie.png", dpi=300)
    plt.close()
    print("-> SUCCESS")
except Exception as e:
    print("-> ERROR", e)

print("FINISH")
#