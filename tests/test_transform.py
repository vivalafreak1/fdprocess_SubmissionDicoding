import pandas as pd
import numpy as np
from utils.transform import transform_data, clean_price, clean_rating, clean_colors, clean_size, clean_gender
from unittest.mock import MagicMock, patch

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

def test_clean_price_exception_path():
    s = pd.Series(['$1', '$2'])
    # Patch .str agar akses .str memicu exception
    with patch.object(pd.Series, 'str', create=True) as mock_str:
        mock_str.__get__ = MagicMock(side_effect=Exception("str fail"))
        out = clean_price(s)
        assert out.isna().all()

def test_clean_rating_exception_path():
    s = pd.Series(['3.5 / 5', '4.0 / 5'])
    with patch.object(pd.Series, 'str', create=True) as mock_str:
        mock_str.__get__ = MagicMock(side_effect=Exception("str fail"))
        out = clean_rating(s)
        assert out.isna().all()

def test_clean_colors_exception_path():
    s = pd.Series(['3 Colors', '2 Colors'])
    with patch.object(pd.Series, 'str', create=True) as mock_str:
        mock_str.__get__ = MagicMock(side_effect=Exception("str fail"))
        out = clean_colors(s)
        assert (out == 0).all()

def test_clean_size_exception_path():
    s = pd.Series(['Size: M', 'Size: L'])
    with patch.object(pd.Series, 'str', create=True) as mock_str:
        mock_str.__get__ = MagicMock(side_effect=Exception("str fail"))
        out = clean_size(s)
        assert (out == 'Unknown').all()

def test_clean_gender_exception_path():
    s = pd.Series(['Gender: Men', 'Gender: Women'])
    with patch.object(pd.Series, 'str', create=True) as mock_str:
        mock_str.__get__ = MagicMock(side_effect=Exception("str fail"))
        out = clean_gender(s)
        assert (out == 'Unknown').all()

def test_transform_data_global_exception_path():
    real_df = pd.DataFrame  # simpan konstruktor asli

    def df_side_effect(*args, **kwargs):
        # Lempar error HANYA untuk panggilan pertama (saat membuat df dari raw)
        # Setelah itu kembalikan konstruktor asli agar blok except bisa membuat DF kosong
        nonlocal called
        if not called:
            called = True
            raise Exception("df ctor fail")
        return real_df(*args, **kwargs)

    called = False
    with patch('utils.transform.pd.DataFrame', side_effect=df_side_effect):
        out = transform_data([{'Title':'A'}])
        assert list(out.columns) == ['Title','Price','Rating','Colors','Size','Gender','Timestamp']
        assert out.empty
        