# [Programmers-devcourse]
### 데이터 웨어하우스를 이용한 대시보드 구성<br><br>
## 📍주제<br>
### 물류 데이터로 확인하는 우리 동네 택배 현황
ETL 프로세스를 통해 데이터웨어하우스에 데이터를 적재하고 대시보드 구성해 보는 프로젝트
<br/>
<br/>
## 📍목표
물류 데이터를 활용하여 __`지역별 물류 생활 대시보드`__ 를 생성하는 것을 목표로 합니다.<br>
데이터 수집, 전처리, 클라우드 스토리지에 저장, 벌크 업데이트의 __ELT 프로세스를 경험__ 하고, <br>
데이터웨어하우스에 적재된 데이터를 이용하여 __대시보드를 생성__ 합니다.<br><br>
## 📍기대효과
멀티 스레딩, 비동기를 활용하여 대용량의 파일을 효과적으로 수집할 수 있습니다.<br>
Python, SQL 언어로 데이터를 전처리할 수 있습니다.<br>
데이터 웨어하우스(Snowflake)를 활용하여 대용량 파일을 처리할 수  있습니다.<br>
데이터를 분석하고 이를 시각적으로 표현하는 데이터 대시보드(Superset)를 만들 수 있습니다.<br><br>

------------

## 📌사용 데이터
- [서울시 생활 물류 데이터](https://data.seoul.go.kr/dataList/OA-21866/S/1/datasetView.do)<br>
- [우편 물류 데이터(시/도)](https://www.koreapost.go.kr/)<br>
- [우편 물류 데이터(구/동)](https://kdx.kr/data/view/31129)<br>
- [행정구역(읍면동)별/5세별 주민등록인구(2011년~)](https://kosis.kr/statHtml/statHtml.do?orgId=101&tblId=DT_1B04005N&conn_path=I2)<br><br>
## 📌프로젝트 구조
![image](https://github.com/Logistics-Devcourse/Data-ETL/assets/77157003/b4ec0647-5d8c-4744-bf37-bfcc24e1d15e) <br>
1. API, 크롤링을 통한 __데이터 수집 및 데이터 전처리__
2. __클라우드 스토리지에 데이터 저장__ (Google Storage)
3. Google storage에 저장된 파일을 __데이터 웨어하우스에 적재__ (Snowflake로 벌크 업데이트)
4. 데이터 웨어하우스(Snowflake) 대시보드(Superset)에 연결
5. __대시보드 생성 및 데이터 분석__ <br><br>
## 📌사용 기술 및 주요 프레임워크
### 데이터 수집 및 전처리
- Python
- BeautifulSoup
- Pandas<br>
### Cloud Storage
- Google Storage<br>
### Data Warehouse
- Snowflake<br>
### 시각화
- Superset(preset.io)<br>
### 협업
- GitHub Project
- Slack<br><br>

-----------
## 🫵역할
### PM
- 이서림 : GCS 및 Snowflake 환경설정
### 분석 테이블 설계
- 김동연 : 수집 데이터 테이블 설계
### ETL 프로세스(데이터 수집, 전처리, 클라우드 스토리지 저장, 데이터웨어하우스 적재)
- 이서림 : `우편 물류 데이터(구/동)`
- 정세욱 : `지역별 인구 수`
- 최은서 : `서울시 생활 물류 데이터` - [스크립트 파일로 자동화(데이터 수집 및 전처리, 클라우드 스토리지 저장)](https://github.com/Logistics-Devcourse/Data-ETL/blob/main/AUTO_ETL_seoul_life_logistics.py)<br>
            `우편 물류 데이터(시/도)` - [데이터 수집 및 전처리](https://github.com/Logistics-Devcourse/Data-ETL/blob/main/Scrapping_sido_delivery_info.ipynb)
### (공통) 대시보드 생성 및 데이터 분석

