import requests
from google.cloud import storage
from datetime import datetime, timedelta

# 사이트에 로그인 하기 위한 세션 생성

s = requests.Session()
headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Authorization": "kdx2023checker",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "DNT": "1",
    "Host": "kdx.kr",
    "Origin": "https://kdx.kr",
    "Referer": "https://kdx.kr/login",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "macOS",
}

# 로그인 정보
payload = {
    "account": "imsolem1226",
    "password": "password",
}

# 구글 클라우드 스토리지에 접근하기 위한 인증
storage_client = storage.Client()

bucket_name = "test-devcourse"  # 여기에 실제 버킷 이름을 입력하세요
bucket = storage_client.bucket(bucket_name)

# 다운로드 받을 파일의 이름을 리스트로 저장
delta = timedelta(days=1)
dates = []

# 2022년도 파일 리스트
start_date = datetime.strptime("20220101", "%Y%m%d")
end_date = datetime.strptime("20221231", "%Y%m%d")

while start_date <= end_date:
    dates.append(start_date.strftime("%Y%m%d"))
    start_date += delta

# 2021년도 파일 리스트
start_date2 = datetime.strptime("20201001", "%Y%m%d")
end_date2 = datetime.strptime("20210930", "%Y%m%d")
dates2 = []
while start_date2 <= end_date2:
    dates2.append(start_date2.strftime("%Y%m%d"))
    start_date2 += delta


params = [
    {
        "year": 2022,
        "product_id": 33153,
        "filenames": dates,
    },
    {
        "year": 2021,
        "product_id": 31129,
        "filenames": dates2,
    },
    {
        "year": 2020,
        "product_id": 27134,
        "filenames": [f"POS_POST ({num}).csv" for num in range(1, 313)],
    },
]


try:
    res = s.post("https://kdx.kr/auth/autoLogin", headers=headers, data=payload)
    res.raise_for_status()  # This will raise an exception if the response status is not ok

    for param in params:
        year = param["year"]
        product_id = param["product_id"]
        filenames = param["filenames"]

        for filename in filenames:
            if year == 2020:
                file_url = f"https://kdx.kr/product/downloadResource?product_id={product_id}&filename={filename}"
            else:
                file_url = f"https://kdx.kr/product/downloadResource?product_id={product_id}&filename=POS00009T_{filename}.csv"

            response = s.get(file_url, stream=True)
            response.raise_for_status()  # This will raise an exception if the response status is not ok

            if year != 2020:
                filename = filename + ".csv"

            blob = bucket.blob(f"{year}/{filename}")
            blob.upload_from_string(response.content)

except requests.exceptions.HTTPError as errh:
    print("Http Error:", errh)
except requests.exceptions.ConnectionError as errc:
    print("Error Connecting:", errc)
except requests.exceptions.Timeout as errt:
    print("Timeout Error:", errt)
except requests.exceptions.RequestException as err:
    print("Something went wrong", err)
