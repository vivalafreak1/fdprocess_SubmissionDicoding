import pandas as pd
import numpy as np

def clean_price(s):
    s = s.replace('Price Unavailable', np.nan)
    # Handle case: semuanya NaN/null, skip .str
    if not pd.api.types.is_string_dtype(s):
        return s
    num = s.str.replace('$','',regex=False).str.replace(',','',regex=False)
    num = pd.to_numeric(num, errors='coerce') * 16000
    return num

def clean_rating(s):
    num = s.str.extract(r'(\d+\.?\d*)')[0]
    return pd.to_numeric(num, errors='coerce')

def clean_colors(s):
    num = s.str.extract(r'(\d+)')[0]
    return pd.to_numeric(num, errors='coerce').fillna(0).astype(int)

def clean_size(s):
    return s.str.replace('Size:','',regex=False).str.strip()

def clean_gender(s):
    return s.str.replace('Gender:','',regex=False).str.strip()

def transform_data(raw):
    df = pd.DataFrame(raw)
    # filter dulu sebelum bersihkan
    df = df[df['Title']!='Unknown Product']
    df = df.reset_index(drop=True)
    if len(df) == 0:
        return df  # Kosong tidak perlu transformasi lagi
    df['Price'] = clean_price(df['Price'])
    df['Rating'] = clean_rating(df['Rating'])
    df['Colors'] = clean_colors(df['Colors'])
    df['Size'] = clean_size(df['Size'])
    df['Gender'] = clean_gender(df['Gender'])
    df = df[df['Title']!='Unknown Product']
    df = df.dropna()
    df = df.drop_duplicates().reset_index(drop=True)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    return df
