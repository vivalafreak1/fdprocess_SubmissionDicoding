import pandas as pd
import os
import pytest
from utils.load import save_csv, save_google_sheets
from unittest.mock import patch, MagicMock

def test_save_csv(tmp_path):
    df = pd.DataFrame({'A':[1]})
    path = tmp_path/'out.csv'
    res = save_csv(df,str(path))
    assert os.path.exists(res)

@patch('utils.load.gspread.authorize')
@patch('utils.load.Credentials.from_service_account_file')
def test_save_google(mon_creds, mon_auth):
    df = pd.DataFrame({'A':[1],'Timestamp':['2025']})
    mock_client = MagicMock()
    mock_sh = MagicMock()
    mock_sh.url = 'url'
    mock_sh.sheet1 = MagicMock()
    mock_client.open.return_value=mock_sh
    mon_auth.return_value=mock_client
    res = save_google_sheets(df,'Name','creds.json')
    assert 'url' in res or res is None

def test_save_csv_io_error(monkeypatch):
    df = pd.DataFrame({'A':[1]})
    def raise_os_error(*args, **kwargs): raise OSError("Disk full")
    monkeypatch.setattr(df, "to_csv", raise_os_error)
    assert save_csv(df, "badfile.csv") is False

@patch('utils.load.Credentials.from_service_account_file')
@patch('utils.load.gspread.authorize')
def test_save_google_sheets_create_spreadsheet(mock_authorize, mock_creds):
    import gspread  # penting: gunakan tipe Exception yg tepat
    mock_client = MagicMock()
    mock_sh = MagicMock()
    mock_ws = MagicMock()
    mock_sh.sheet1 = mock_ws
    mock_sh.url = 'https://sheet.url'
    created = {'v': False}

    def create_side_effect(name):
        created['v'] = True
        return mock_sh

    mock_authorize.return_value = mock_client
    mock_creds.return_value = object()

    mock_client.open.side_effect = gspread.SpreadsheetNotFound("not found")
    mock_client.create.side_effect = create_side_effect

    url = save_google_sheets(pd.DataFrame({'A':[1], 'Timestamp':['2025']}), 'sheet_name', 'creds.json')
    assert created['v'] is True
    assert url == 'https://sheet.url'

