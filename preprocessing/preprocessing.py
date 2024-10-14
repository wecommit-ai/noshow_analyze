import mysql.connector
import pandas as pd
import pickle
import argparse
import os
# SettingWithCopyWarning 제거(경고 메세지)
pd.options.mode.chained_assignment = None 

# 업데이트 되는 파일이 MySQL의 가장 최신 파일이라고 산정했을 때
# SQL 접속 및 최신 파일 찾기
def fetch_latest_file_from_mysql():
    connection = mysql.connector.connect(
        host='localhost',
        user='username',
        password='password',
        database='database_name'
    )
    
    # 최신 파일을 선택하는 쿼리
    query = """
    SELECT file_name, upload_date
    FROM files
    ORDER BY upload_date DESC
    LIMIT 1
    """
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    latest_file = cursor.fetchone()
    connection.close()
    
    if latest_file:
        return latest_file['file_name']
    else:
        raise Exception("No files found in the database.")

# MySQL에서 최신 파일을 다운로드하거나 쿼리로 데이터 추출하기
def fetch_data_from_mysql(file_name):
    connection = mysql.connector.connect(
        host='localhost',
        user='username',
        password='password',
        database='database_name'
    )
    
    # 최신 파일에서 데이터를 추출하는 쿼리
    query = f"SELECT * FROM {file_name}"
    df = pd.read_sql(query, connection)
    connection.close()
    return df

# XLSX 파일에서 데이터를 가져오는 함수
def fetch_data_from_xlsx(file_path):
    df = pd.read_excel(file_path)
    return df

# 전처리 로직(임시)
def preprocess_data(df):
    df = df.copy()
    # "original_link" 기준 중복 행 제거
    df = df.drop_duplicates(subset=['original_link']).reset_index(drop=True)
    # 키워드 기반(TITLE) 필터링
    df = df[~df['title'].str.contains('임대|야놀자|입장권|상품권|포인트|야놀|주차권|쿠폰|구매|비행기|종일권|자유이용권', na=False)]
    # 카테고리 결측값 대체
    df['category'] = df['category'].fillna("여행숙박/이용권")
    # 카테고리 기반 필터링
    df = df[(df['category'] =='여행/숙박/렌트') | (df['category'] =='티켓/교환권') | (df['category']== '여행숙박/이용권') | (df['category']== '기타 티켓/쿠폰/이용권')]
    # 모든 값이 결측값인 행 제거
    df.dropna(axis=0, how='all', inplace=True)
    # 결측값 대체
    df['description'] = df['description'].fillna('정보없음')
    # Price 이상치 제거
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df = df.query('10000 <= price <= 5000000')
    # "seller_location" 필드 세분화
    df['city'], df['city_goo'], df['city_dong'] = zip(*df['seller_location'].apply(split_location))
    df['city'] = df['city'].astype('object')
    # 구 기준 "서울특별시" 값 추가
    seoul_goo = ["강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구", "노원구", "도봉구", "동대문구", 
                 "동작구", "마포구", "서대문구", "서초구", "성동구", "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", 
                 "종로구", "중구", "중랑구"]
    df.loc[df['city_goo'].isin(seoul_goo) & df['city'].isna(), 'city'] = '서울특별시'
    # 중고나라 images 링크 짤리는 현상 처리
    df['images'] = df['images'].apply(lambda x: x.split(',') if isinstance(x, str) else x)
    df['images'] = df.apply(
        lambda row: [
            f"https://img2.joongna.com{img.strip()}" if img.strip().startswith('/') else img.strip() 
            for img in row['images']
        ] 
        if row['platform'] == '중고나라' else row['images'], 
        axis=1
    )
    df = df.reset_index(drop=True)
    return df

# 위치 데이터를 분리하는 함수
def split_location(location):
    if pd.isna(location):
        return (None, None, None)

    location_parts = location.strip().split(" ")
    si, goo, dong = None, None, None
    
    for part in location_parts:
        clean_part = part.strip()
        if clean_part.endswith('시'):
            si = clean_part
        elif clean_part.endswith('구'):
            goo = clean_part
        elif clean_part.endswith('동'):
            dong = clean_part
    
    return (si, goo, dong)


def save_to_pickle(df, base_filename='processed_data.pkl'):
    filename = base_filename
    counter = 1

    while os.path.exists(filename):
        filename = f'processed_data_{counter}.pkl'
        counter += 1

    with open(filename, 'wb') as f:
        pickle.dump(df, f)
    
    print(f"Data saved as {filename}")

# 메인 파이프라인
def main(mode, file_path=None):
    try:
        if mode == 'mysql':
            latest_file_name = fetch_latest_file_from_mysql()
            print(f"SQL Latest file: {latest_file_name}")
            # 최신 파일의 데이터를 가져오기
            df = fetch_data_from_mysql(latest_file_name)
        elif mode == 'xlsx':
            if not file_path:
                raise ValueError("File path must be provided for XLSX mode")
            print(f"XLSX file: {file_path}")
            df = fetch_data_from_xlsx(file_path)
        else:
            raise ValueError("Invalid mode. Choose 'mysql' or 'xlsx'.")
        
        # 데이터 전처리
        processed_df = preprocess_data(df)
        
        # 전처리된 데이터를 원하는 형식으로 저장
        save_to_pickle(processed_df)
        
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == '__main__':
    # argparse를 사용하여 터미널에서 인자 받기
    parser = argparse.ArgumentParser(description="Process data from MySQL or XLSX and save as pickle.")
    parser.add_argument('--mode', choices=['mysql', 'xlsx'], required=True, help="Choose the data source mode: 'mysql' or 'xlsx'")
    parser.add_argument('--file', type=str, help="The path to the XLSX file if mode is 'xlsx'")
    
    args = parser.parse_args()

    # main 함수 실행
    main(mode=args.mode, file_path=args.file)