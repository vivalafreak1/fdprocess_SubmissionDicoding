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

@patch('utils.load.Credentials.from_service_account_file')
@patch('utils.load.gspread.authorize')
def test_save_google_sheets_auth_exception(mock_auth, mock_creds):
    # Paksa kredensial melempar error untuk memukul except kredensial/authorize (18-21)
    mock_creds.side_effect = FileNotFoundError("no creds")
    with pytest.raises(FileNotFoundError):
        save_google_sheets(pd.DataFrame({'Timestamp':['2025']}), 'name', 'missing.json')

@patch('utils.load.Credentials.from_service_account_file')
@patch('utils.load.gspread.authorize')
def test_save_google_sheets_update_error(mock_authorize, mock_creds):
    # Setup client dan sheet sukses, tapi update() melempar error
    mock_client = MagicMock()
    mock_sh = MagicMock()
    mock_ws = MagicMock()
    mock_sh.sheet1 = mock_ws
    mock_sh.url = 'https://sheet.url'
    mock_client.open.return_value = mock_sh

    mock_authorize.return_value = mock_client
    mock_creds.return_value = object()

    # Paksa update melempar
    mock_ws.update.side_effect = Exception("update fail")

    # Biarkan exception propagate (fungsi tidak menangkap ini), sehingga test memukul baris 20–21
    with pytest.raises(Exception):
        save_google_sheets(pd.DataFrame({'Timestamp':['2025']}), 'sheet', 'creds.json')

@patch('utils.load.Credentials.from_service_account_file')
@patch('utils.load.gspread.authorize')
def test_save_google_sheets_happy_path_calls_update(mock_authorize, mock_creds):
    # Arrange: authorize OK, open OK, ws.clear dan ws.update terpanggil
    mock_client = MagicMock()
    mock_sh = MagicMock()
    mock_ws = MagicMock()
    mock_sh.sheet1 = mock_ws
    mock_sh.url = 'https://sheet.url'
    mock_client.open.return_value = mock_sh

    mock_authorize.return_value = mock_client
    mock_creds.return_value = object()

    df = pd.DataFrame({'A':[1], 'Timestamp':['2025-01-01T00:00:00']})

    # Act
    url = save_google_sheets(df, 'Existing Sheet', 'creds.json')

    # Assert: memastikan clear dan update (baris 20–21) dieksekusi
    mock_ws.clear.assert_called_once()
    assert mock_ws.update.call_count == 1
    assert url == 'https://sheet.url'

@patch('utils.load.Credentials.from_service_account_file')
@patch('utils.load.gspread.authorize')
def test_save_google_sheets_create_branch_updates_after_share(mock_authorize, mock_creds):
    import gspread
    # Arrange: authorize OK
    mock_client = MagicMock()
    # open() melempar SpreadsheetNotFound supaya masuk cabang except
    mock_client.open.side_effect = gspread.SpreadsheetNotFound("not found")

    # create() mengembalikan sheet baru
    mock_new_sh = MagicMock()
    mock_ws = MagicMock()
    mock_new_sh.sheet1 = mock_ws
    mock_new_sh.url = 'https://created-sheet.url'
    mock_client.create.return_value = mock_new_sh

    mock_authorize.return_value = mock_client
    mock_creds.return_value = object()

    df = pd.DataFrame({'A':[1], 'Timestamp':['2025-01-01T00:00:00']})

    # Act
    url = save_google_sheets(df, 'New Sheet', 'creds.json')

    # Assert: pastikan cabang except berjalan (create + share), lalu update dipanggil
    mock_client.open.assert_called_once()
    mock_client.create.assert_called_once_with('New Sheet')
    mock_new_sh.share.assert_called_once()          # baris di cabang except
    mock_ws.update.assert_called_once()             # baris setelah cabang except
    assert url == 'https://created-sheet.url'

@patch('utils.load.Credentials.from_service_account_file')
@patch('utils.load.gspread.authorize')
def test_save_google_sheets_create_branch_full_flow(mock_authorize, mock_creds, monkeypatch):
    import gspread
    mock_client = MagicMock()

    # open gagal → masuk except
    mock_client.open.side_effect = gspread.SpreadsheetNotFound("nf")

    # objek hasil create
    mock_new_sh = MagicMock()
    mock_ws = MagicMock()
    mock_new_sh.sheet1 = mock_ws
    mock_new_sh.url = 'https://created.url'
    mock_client.create.return_value = mock_new_sh

    mock_authorize.return_value = mock_client
    mock_creds.return_value = object()

    # Spying df.copy agar jalur setelah except benar-benar dieksekusi
    df = pd.DataFrame({'A':[1], 'Timestamp':['2025-01-01T00:00:00']})
    copy_called = {'v': False}
    real_copy = df.copy
    def copy_wrapper(*a, **k):
        copy_called['v'] = True
        return real_copy(*a, **k)
    monkeypatch.setattr(df, "copy", copy_wrapper)

    url = save_google_sheets(df, 'Brand New', 'creds.json')

    # Verifikasi urutan dan objek yang dipakai
    mock_client.open.assert_called_once()
    mock_client.create.assert_called_once_with('Brand New')
    mock_new_sh.share.assert_called_once_with(None, perm_type='anyone', role='writer')  # baris 20
    assert copy_called['v'] is True                                                     # baris 21 (df.copy)
    mock_ws.update.assert_called_once()
    assert url == 'https://created.url'

@patch('utils.load.gspread.authorize')
@patch('utils.load.Credentials.from_service_account_file')
def test_save_google_sheets_create_branch_hits_share_and_copy(mock_creds, mock_authorize, monkeypatch):
    import gspread
    mock_client = MagicMock()
    mock_client.open.side_effect = gspread.SpreadsheetNotFound("nf")

    mock_sh = MagicMock()
    mock_ws = MagicMock()
    mock_sh.sheet1 = mock_ws
    mock_sh.url = 'https://created.url'
    mock_client.create.return_value = mock_sh

    mock_authorize.return_value = mock_client
    mock_creds.return_value = object()

    df = pd.DataFrame({'A':[1], 'Timestamp':['2025-01-01T00:00:00']})
    copy_called = {'v': False}
    real_copy = df.copy
    def copy_wrapper(*args, **kwargs):
        copy_called['v'] = True
        return real_copy(*args, **kwargs)
    monkeypatch.setattr(df, "copy", copy_wrapper)

    url = save_google_sheets(df, 'Brand New', 'creds.json')

    mock_client.open.assert_called_once()
    mock_client.create.assert_called_once_with('Brand New')
    mock_sh.share.assert_called_once_with(None, perm_type='anyone', role='writer')
    assert copy_called['v'] is True
    mock_ws.update.assert_called_once()
    assert url == 'https://created.url'