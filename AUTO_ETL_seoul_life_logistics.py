# 1. Google Cloud Storage API 연결
import os
from google.cloud import storage
from datetime import datetime
from io import StringIO
import requests
import pandas as pd
import calendar
from io import BytesIO
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed


def increment_month(input_month):
    year = int(input_month[:4])
    month = int(input_month[4:])
    if month == 12:
        year += 1
        next_month = str(year) + "01"
    else:
        next_month = str(year) + str(month + 1).zfill(2)
    return next_month


def generate_date_list(now_month, last_day):
    date_list = []

    # 문자열 형식의 now_month를 datetime 객체로 변환
    start_date = datetime.strptime(now_month, "%Y%m")

    # 각 날짜를 리스트에 추가
    for day in range(1, last_day + 1):
        current_date = start_date + timedelta(days=day - 1)
        date_list.append(current_date.strftime("%Y%m%d"))

    return date_list


def fetch_data(url):
    res = requests.get(url)
    data = res.json()
    return data


def api_call(url, key):
    # print(url)
    res = requests.get(url)
    data = res.json()

    if 'RESULT' in data.keys():
        # print(url)
        # print("이상")
        # print(data['RESULT'])
        return pd.DataFrame()
    return pd.DataFrame(data['seoulGuGu']['row'])


def makemonthdf(folder_name, url, now_month):
    if folder_name == 'SEOUL_SEOUL':
        key = 'seoulGuGu'
        # 따로 추가 필요 없음
    elif folder_name == 'SEOUL_SIDO':
        key = 'seoulGuSido'
        new_column_name = 'REC_CTGG_NM'
        insert_idx = 7
    elif folder_name == 'SIDO_SEOUL':
        key = 'sidoSeoulgu'
        new_column_name = 'SEND_CTGG_NM'
        insert_idx = 3
    else:
        return None

    startidx = 1
    date_format = "%Y%m%d"

    start_month = now_month

    start_date = datetime.strptime(start_month + '01', date_format)
    end_date = start_date.replace(day=calendar.monthrange(
        start_date.year, start_date.month)[1])

    url_list = [f"{url}{startidx}/{startidx + 999}/{date.strftime('%Y%m%d')}"
                for date in (start_date + timedelta(n) for n in range((end_date - start_date).days + 1))
                ]

    # 마지막날이 존재하지 않으면(아직 삽입하지 않음)
    if api_call(url_list[-1], key).empty:
        print(str(start_month)+"의 마지막날의 데이터가 없습니다")
    else:
        cpu_count = os.cpu_count()
        with ThreadPoolExecutor(max_workers=(cpu_count-2)) as executor:
            futures = [executor.submit(api_call, url, key) for url in url_list]

            results = []
            for future in as_completed(futures):
                result = future.result()
                results.append(result)

        try:
            final_result = pd.concat(results, ignore_index=True)
        except pd.errors.EmptyDataError:
            final_result = pd.DataFrame()  # 빈 데이터프레임으로 초기화
            print("데이터가 없습니다.")
        else:
            # insert
            if folder_name in ['SEOUL_SIDO', 'SIDO_SEOUL']:
                null_values = [None] * len(final_result)
                final_result.insert(loc=insert_idx, column=new_column_name,
                                    value=null_values)
            final_result = final_result[['DL_YMD', 'SEND_CTPV_NM', 'SEND_CTGG_NM', 'REC_CTPV_NM', 'REC_CTGG_NM',
                                        'E_C_01', 'E_C_02', 'E_C_03', 'E_C_04', 'E_C_05', 'E_C_06', 'E_C_07', 'E_C_08', 'E_C_09', 'E_C_10', 'E_C_11']]
            conversion_rules = {'E_C_01': int, 'E_C_02': int, 'E_C_03': int, 'E_C_04': int, 'E_C_05': int,
                                'E_C_06': int, 'E_C_07': int, 'E_C_08': int, 'E_C_09': int, 'E_C_10': int, 'E_C_11': int}
            final_result = final_result.astype(conversion_rules)
            final_result.rename(columns={'DL_YMD': 'delivery_date', 'SEND_CTPV_NM': 'sender_city', 'SEND_CTGG_NM': 'sender_district', 'REC_CTPV_NM': 'recipient_city', 'REC_CTGG_NM': 'recipient_district',
                                         'E_C_05': 'category_living_and_health', 'E_C_06': 'category_sports_and_leisure', 'E_C_07': 'category_food', 'E_C_08': 'category_parenting',
                                         'E_C_09': 'category_fashion_clothing', 'E_C_10': 'category_fashion_accessories', 'E_C_11': 'category_beauty_cosmetics'}, inplace=True)
            return final_result


def savecsv(df, file_name, bucket_name, folder_path):
    with open('config.json') as config_file:
        config = json.load(config_file)

    googlo_cloud_api_key = config.get('GOOGLE_APPLICATION_CREDENTIALS')

    if googlo_cloud_api_key is None:
        print("Google Cloud API 키를 찾을 수 없습니다.")
    else:
        # 1-1) 환경변수 - Googlecloud_key 설정
        storage_client = storage.Client.from_service_account_json(
            googlo_cloud_api_key)
        bucket = storage_client.get_bucket(bucket_name)

        blob_path = f"{folder_path}/{file_name}"
        blob = bucket.blob(blob_path)

        # CSV 데이터를 문자열로 변환
        try:
            csv_string = df.to_csv(index=False, encoding='utf-8')  # UTF-8로 인코딩
        except AttributeError:
            print("31일까지 데이터가 없습니다")
        else:
            # 문자열을 BytesIO로 감싸서 업로드
            csv_bytes = BytesIO(csv_string.encode(
                'utf-8'))  # UTF-8로 인코딩된 바이트로 변환
            blob.upload_from_file(csv_bytes, content_type='text/csv')

            print(f"File {blob_path} uploaded to {bucket_name}.")


def check(bucket_name, folder_name, lastmonth):
    startmonth = "202101"
    with open('config.json') as config_file:
        config = json.load(config_file)
        googlo_cloud_api_key = config.get('GOOGLE_APPLICATION_CREDENTIALS')

        if googlo_cloud_api_key is None:
            print("API 키를 찾을 수 없습니다.")
        else:
            # 1-1) 환경변수 - Googlecloud_key 설정
            storage_client = storage.Client.from_service_account_json(
                googlo_cloud_api_key)
            folder_path = 'Seoul-Life-Logistics/'+folder_name+'/'
            # 특정 버킷 가져오기
            bucket = storage_client.get_bucket(bucket_name)
            # 폴더 내 객체(파일)목록 가져오기
            blobs = bucket.list_blobs(prefix=folder_path)
            blob_list = []

            for blob in blobs:
                blob_list.append(blob.name)
            blob_list.sort()

            existing_months = set()
            for blob_name in blob_list:
                date_str = blob_name.split('_')[-1].split('.')[0]
                existing_months.add(date_str)

            # startmonth와 lastmonth를 datetime 객체로 변환
            start_date = datetime.strptime(startmonth, "%Y%m")
            last_date = datetime.strptime(lastmonth, "%Y%m")

            all_months = set((start_date.replace(day=1) + timedelta(days=30 * month)).strftime("%Y%m")
                             for month in range((last_date.year - start_date.year) * 12 + last_date.month))

            # 존재하지 않는 달 찾기
            missing_months = list(all_months - existing_months)
            missing_months.sort()
            return missing_months


# 2-1. (서울 공공데이터 포탈) 서울시 생활물류
def main():
    with open('config.json') as config_file:
        config = json.load(config_file)
        seoul_api_key = config.get('seoul_api_key')
        googlo_cloud_api_key = config.get('GOOGLE_APPLICATION_CREDENTIALS')

    if seoul_api_key is None:
        print("API 키를 찾을 수 없습니다.")
    elif googlo_cloud_api_key is None:
        print("Google Cloud API 키를 찾을 수 없습니다.")
    else:
        # 1-1) 환경변수 - Googlecloud_key 설정
        storage_client = storage.Client.from_service_account_json(
            googlo_cloud_api_key)
        buckets = list(storage_client.list_buckets())

        # 2. 현재 시간과 저장된 시간 확인
        nowdate = str(datetime.today().year) + \
            str(datetime.today().month)+str(datetime.today().day)
        lastmonth = str(datetime.today().year)+str(datetime.today().month-1)

        bucket_name = 'programmers-devcourse-project2'
        folder_list = ['SEOUL_SEOUL', 'SEOUL_SIDO', 'SIDO_SEOUL']
        folder_dict = {'SEOUL_SEOUL': ["http://openapi.seoul.go.kr:8088/"+seoul_api_key+"/json/seoulGuGu/", "SEOULGU_SEOULGU"],
                       'SEOUL_SIDO': ["http://openapi.seoul.go.kr:8088/"+seoul_api_key+"/json/seoulGuSido/", "SEOULGU_SIDO"],
                       'SIDO_SEOUL': ["http://openapi.seoul.go.kr:8088/"+seoul_api_key+"/json/sidoSeoulgu/", "SIDO_SEOULGU"]}

        for folder in folder_list:
            folder_path = 'Seoul-Life-Logistics/'+folder
            missing_month = check(bucket_name, folder, lastmonth)
            if len(missing_month) > 0:
                for now_month in missing_month:
                    print(folder+" "+now_month)
                    # makemonthdf(folder_name, url, month_list)
                    df = makemonthdf(
                        folder, folder_dict[folder][0], now_month)
                    file_name = "DWC_KXLCLS_OD_DAY_" + \
                        folder_dict[folder][1]+"_"+now_month+".csv"
                    # savecsv(df, file_name, bucket_name, folder_name):
                    savecsv(df, file_name, bucket_name, folder_path)


if __name__ == "__main__":
    main()
