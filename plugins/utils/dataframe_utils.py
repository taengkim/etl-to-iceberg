"""DataFrame 처리 유틸리티"""
import pandas as pd


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame 데이터 정리 및 변환
    
    :param df: 원본 DataFrame
    :return: 정리된 DataFrame
    """
    # 컬럼명을 소문자로 변환
    df.columns = df.columns.str.lower()
    
    # None을 NaN으로 변환
    df = df.where(pd.notnull(df), None)
    
    # datetime 컬럼 타입 확인
    for col in df.columns:
        if df[col].dtype == 'object':
            # datetime 문자열일 수 있으므로 시도
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass
    
    return df
