import pandas as pd
import haversine as hs


def process_geo_data(geo):
    # 각 시트를 순회하며 데이터 처리
    for sheet in geo:
        drop_list = []
        for i in geo[sheet].index:
            # '읍/면/리/동' 컬럼이 NaN이 아니고, 마지막 글자가 '리'인 경우 drop_list에 추가
            if not pd.isna(geo[sheet].loc[i, "읍/면/리/동"]):
                if geo[sheet].loc[i, "읍/면/리/동"][-1] == "리":
                    drop_list.append(i)
            # '리' 컬럼이 NaN이 아닌 경우 drop_list에 추가
            if not pd.isna(geo[sheet].loc[i, "리"]):
                drop_list.append(i)
        # drop_list에 있는 행을 제거
        for i in drop_list:
            geo[sheet].drop(i, inplace=True)

    # 각 시트를 순회하며 데이터 처리
    for sheet in geo:
        # 인덱스를 재설정
        geo[sheet].reset_index(inplace=True)
        for i in geo[sheet].index:
            # '읍/면/리/동' 컬럼이 NaN이 아닌 경우
            if not pd.isna(geo[sheet].loc[i, "읍/면/리/동"]):
                si = geo[sheet].loc[i, "시군구"]
                gu = geo[sheet].loc[i, "읍면동/구"]
                dong = geo[sheet].loc[i, "읍/면/리/동"]
                # '시군구' 컬럼과 '읍면동/구' 컬럼을 합침
                geo[sheet].loc[i, "시군구"] = si + " " + gu
                # '읍/면/리/동' 컬럼의 값을 '읍면동/구' 컬럼에 저장
                geo[sheet].loc[i, "읍면동/구"] = dong

    # 모든 시트의 데이터를 합침
    all_data = pd.concat(geo.values(), ignore_index=True)
    return all_data


def process_geo_regioncode(geo2, code):
    for i in geo2.index:
        try:
            sido_name = geo2.loc[i, "시도"]
            sigungu_name = geo2.loc[i, "시군구"]
            dong_name = geo2.loc[i, "읍면동/구"]
            sido_code = code.loc[code["시도명칭"] == sido_name, "시도코드"].values[0]
            sigungu_code = code.loc[
                (code["시군구명칭"] == sigungu_name) & (code["시도명칭"] == sido_name),
                "시군구코드",
            ].values[0]
            if pd.isna(geo2.loc[i, "읍/면/리/동"]):
                dong_code = 0
            else:
                dong_name = geo2.loc[i, "읍/면/리/동"]
                geo2.loc[i, "읍면동/구"] = dong_name
                dong_code = code.loc[
                    (code["시군구명칭"] == sigungu_name)
                    & (code["시도명칭"] == sido_name)
                    & (code["읍면동명칭"] == dong_name),
                    "읍면동코드",
                ].values[0]
            geo2.loc[i, "행정구역 코드"] = (
                str(sido_code)
                + "{:03}".format(sigungu_code)
                + "{:03}".format(dong_code)
            )
        except IndexError:
            continue

    geo2 = geo2.drop("읍/면/리/동", axis=1)
    geo2 = geo2.drop("리", axis=1)

    geo2["행정구역 코드"] = pd.to_numeric(geo2["행정구역 코드"], errors="coerce")
    geo2 = geo2.dropna(subset=["행정구역 코드"])
    geo2["행정구역 코드"] = geo2["행정구역 코드"].astype(int)

    return geo2


def calculate_distance(geo3):
    distances = []
    locations = geo3[["위도", "경도"]].values
    region_codes = geo3["행정구역 코드"].values

    for i in range(len(locations)):
        for j in range(i + 1, len(locations)):
            distance = hs.haversine(locations[i], locations[j])
            distances.append(
                {
                    "src_region": region_codes[i],
                    "dest_region": region_codes[j],
                    "distance": distance,
                }
            )
            distances.append(
                {
                    "src_region": region_codes[j],
                    "dest_region": region_codes[i],
                    "distance": distance,
                }
            )

    distances_df = pd.DataFrame(distances)

    return distances_df


def main():
    # geo 데이터를 로드
    geo = pd.read_excel("dataset/hangjeong_geo.xlsx", sheet_name=None)
    region_code = pd.read_excel("dataset/hangjeong_code.xlsx", sheet_name=0)
    # geo 데이터를 처리
    all_data = process_geo_data(geo)

    region_coordinate_code = process_geo_regioncode(all_data, region_code)

    region_coordinate_code.to_csv("region_codes.csv", index=False)

    distances_df = calculate_distance(region_coordinate_code)
    distances_df.to_csv("dataset/distances.csv", index=False)


if __name__ == "__main__":
    # 이 스크립트가 직접 실행될 때만 main 함수를 호출
    main()
