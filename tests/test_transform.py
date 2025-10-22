import pandas as pd
import numpy as np
from utils.transform import transform_data, clean_price

def test_transform_success():
    raw = [{
        'Title':'A','Price':'$2.50','Rating':'3.5 / 5',
        'Colors':'2 Colors','Size':'Size: L','Gender':'Gender: M','Timestamp':'2025-10-22T00:00:00'
    }]
    df = transform_data(raw)
    assert df.iloc[0]['Price']==2.50*16000
    assert df.iloc[0]['Rating']==3.5
    assert df.iloc[0]['Colors']==2
    assert df.iloc[0]['Size']=='L'
    assert df.iloc[0]['Gender']=='M'

def test_transform_filter_invalid():
    raw = [{
        'Title':'Unknown Product','Price':'Price Unavailable','Rating':'Invalid','Colors':'0 Colors',
        'Size':'Size: ?','Gender':'Gender: ?','Timestamp':'2025-10-22T00:00:00'
    }]
    df = transform_data(raw)
    assert df.empty

def test_clean_price_non_string():
    s = pd.Series([np.nan, np.nan])
    out = clean_price(s)
    assert out.isna().all()