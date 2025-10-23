import pandas as pd
import numpy as np

def clean_price(s):
    try:
        s = s.replace('Price Unavailable', np.nan)
        if not pd.api.types.is_string_dtype(s):
            return s
        num = s.str.replace('$','',regex=False).str.replace(',','',regex=False)
        num = pd.to_numeric(num, errors='coerce') * 16000
        return num
    except Exception:
        return pd.to_numeric(pd.Series([np.nan]*len(s)), errors='coerce')

def clean_rating(s):
    try:
        num = s.str.extract(r'(\d+\.?\d*)')[0]
        return pd.to_numeric(num, errors='coerce')
    except Exception:
        return pd.to_numeric(pd.Series([np.nan]*len(s)), errors='coerce')

def clean_colors(s):
    try:
        num = s.str.extract(r'(\d+)')[0]
        return pd.to_numeric(num, errors='coerce').fillna(0).astype(int)
    except Exception:
        return pd.Series([0]*len(s), dtype=int)

def clean_size(s):
    try:
        return s.str.replace('Size:','',regex=False).str.strip()
    except Exception:
        return pd.Series(['Unknown']*len(s), dtype='object')

def clean_gender(s):
    try:
        return s.str.replace('Gender:','',regex=False).str.strip()
    except Exception:
        return pd.Series(['Unknown']*len(s), dtype='object')

def transform_data(raw):
    try:
        df = pd.DataFrame(raw)
        df = df[df['Title']!='Unknown Product'].reset_index(drop=True)
        if len(df) == 0:
            return df
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
    except Exception:
        # jika transform gagal total, kembalikan df kosong agar tahap load aman
        return pd.DataFrame(columns=['Title','Price','Rating','Colors','Size','Gender','Timestamp'])
