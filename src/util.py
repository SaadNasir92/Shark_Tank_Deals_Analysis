import pandas as pd
import numpy as np

def check_nulls(df):
    summary = {}
    for column in df.columns:
        col_type = df[column].dtype
        if col_type == 'bool':
            summary[column] = df[column].isnull().sum()
        elif col_type in ['int64', 'float64']:
            summary[column] = df[column].isnull().sum() + pd.to_numeric(df[column], errors='coerce').isnull().sum() - df[column].isnull().sum()
        elif col_type == 'object':
            summary[column] = {
                'standard_nulls': df[column].isnull().sum(),
                'empty_strings': (df[column] == '').sum(),
                'specific_values': df[column].isin(['N/A', 'NA', 'None', '-']).sum(),
                'whitespace_only': df[column].str.strip().eq('').sum()
            }
        elif np.issubdtype(col_type, np.datetime64):
            summary[column] = {
                'invalid_dates': pd.to_datetime(df[column], errors='coerce').isnull().sum() - df[column].isnull().sum()
            }
    return summary