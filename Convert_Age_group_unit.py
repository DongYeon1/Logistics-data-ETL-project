import pandas as pd
for i in range(3) :
    df = pd.read_csv(f"101_DT_1B04005N_Y_202{i}.csv", skiprows=2, encoding="cp949", low_memory=False)
    df = df.drop(['C5세별'], axis=1)
    df['5세별'] = df['5세별'].str.replace('세', '')

    # 연령대 단위 변환 함수
    def assign_10_year_bracket(age_bracket) :
        try :
            if age_bracket == '100+' :
                return '100+'

            start_age = int(age_bracket.split(' - ')[0])
            if start_age < 10:
                return '0 - 9'
            elif start_age < 20:
                return '10 - 19'
            elif start_age < 30:
                return '20 - 29'
            elif start_age < 40:
                return '30 - 39'
            elif start_age < 50:
                return '40 - 49'
            elif start_age < 60:
                return '50 - 59'
            elif start_age < 70:
                return '60 - 69'
            elif start_age < 80:
                return '70 - 79'
            elif start_age < 90:
                return '80 - 89'
            else:
                return '90 - 99'
        except ValueError :
            return '계'
        
    df['10세별'] = df['5세별'].apply(assign_10_year_bracket)
    ten_years_df = df.groupby(['10세별', 'C행정구역(동읍면)별', '행정구역(동읍면)별'], sort=False)[['총인구수 (명)', '남자인구수 (명)', '여자인구수 (명)']].sum().reset_index()
    new_order = ['C행정구역(동읍면)별', '행정구역(동읍면)별', '10세별', '총인구수 (명)', '남자인구수 (명)', '여자인구수 (명)']
    ten_years_df = ten_years_df[new_order]
    
    ten_years_df.to_csv(f'Population_by_Region_Age_RegionCodes_202{i}.csv', encoding='cp949', index=False)