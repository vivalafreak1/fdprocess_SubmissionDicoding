import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from utils.load import save_csv, save_google_sheets, make_pg_engine, ensure_products_table, save_postgres

@pytest.fixture
def df_ok():
    return pd.DataFrame({
        "Title": ["A","B"],
        "Price": [1000.0, 2000.0],
        "Rating": [4.0, 3.5],
        "Colors": [2, 3],
        "Size": ["M","L"],
        "Gender": ["Men","Women"],
        "Timestamp": ["2025-01-01T00:00:00","2025-01-02T00:00:00"]
    })

@pytest.fixture
def df_pg_ok():
    return pd.DataFrame({
        "Title": ["A","B"],
        "Price": [1000.0, 2000.0],
        "Rating": [4.0, 3.5],
        "Colors": [2, 3],
        "Size": ["M","L"],
        "Gender": ["Men","Women"],
        "Timestamp": pd.to_datetime(["2025-01-01T00:00:00","2025-01-02T00:00:00"])
    })

@pytest.fixture
def gs_client_mocks():
    mock_client = MagicMock()
    mock_sh = MagicMock()
    mock_ws = MagicMock()
    mock_sh.sheet1 = mock_ws
    mock_sh.url = "https://sheet.url"
    return mock_client, mock_sh, mock_ws

def test_save_csv(tmp_path):
    df = pd.DataFrame({"A":[1]})
    path = tmp_path/"out.csv"
    assert os.path.exists(save_csv(df, str(path)))

def test_save_csv_io_error(monkeypatch):
    df = pd.DataFrame({"A":[1]})
    def boom(*a, **k): raise OSError("disk")
    monkeypatch.setattr(df, "to_csv", boom)
    assert save_csv(df, "bad.csv") is False

@patch("utils.load.gspread.authorize")
@patch("utils.load.Credentials.from_service_account_file")
def test_sheets_happy_path(mock_creds, mock_auth, df_ok, gs_client_mocks):
    mock_client, mock_sh, mock_ws = gs_client_mocks
    mock_client.open.return_value = mock_sh
    mock_auth.return_value = mock_client
    mock_creds.return_value = object()
    assert save_google_sheets(df_ok, "Existing", "creds.json") == "https://sheet.url"
    mock_client.open.assert_called_once_with("Existing")
    mock_ws.clear.assert_called_once()
    assert mock_ws.update.call_count == 1

@patch("utils.load.gspread.authorize")
@patch("utils.load.Credentials.from_service_account_file")
def test_sheets_create_branch(mock_creds, mock_auth, df_ok, gs_client_mocks):
    import gspread
    mock_client, mock_sh, mock_ws = gs_client_mocks
    mock_client.open.side_effect = gspread.SpreadsheetNotFound("nf")
    mock_client.create.return_value = mock_sh
    mock_auth.return_value = mock_client
    mock_creds.return_value = object()
    url = save_google_sheets(df_ok, "NewSheet", "creds.json")
    mock_client.create.assert_called_once_with("NewSheet")
    mock_sh.share.assert_called_once()
    mock_ws.update.assert_called_once()
    assert url == "https://sheet.url"

@patch("utils.load.gspread.authorize")
@patch("utils.load.Credentials.from_service_account_file")
def test_sheets_creds_not_found(mock_creds, mock_auth, df_ok):
    mock_creds.side_effect = FileNotFoundError("missing")
    with pytest.raises(FileNotFoundError):
        save_google_sheets(df_ok, "X", "missing.json")

@patch("utils.load.gspread.authorize")
@patch("utils.load.Credentials.from_service_account_file")
def test_sheets_update_error_raised(mock_creds, mock_auth, df_ok, gs_client_mocks):
    mock_client, mock_sh, mock_ws = gs_client_mocks
    mock_client.open.return_value = mock_sh
    mock_auth.return_value = mock_client
    mock_creds.return_value = object()
    mock_ws.update.side_effect = Exception("update")
    with pytest.raises(Exception):
        save_google_sheets(df_ok, "Existing", "creds.json")

@patch("utils.load.create_engine")
def test_make_pg_engine_ok(mock_ce):
    mock_engine = MagicMock()
    mock_ce.return_value = mock_engine
    ctx = MagicMock()
    ctx.__enter__.return_value = MagicMock()
    mock_engine.connect.return_value = ctx.__enter__.return_value
    mock_engine.connect.return_value.execute.return_value = None
    assert make_pg_engine() is mock_engine

@patch("utils.load.create_engine")
def test_make_pg_engine_fail_connect(mock_ce):
    mock_engine = MagicMock()
    mock_ce.return_value = mock_engine
    mock_engine.connect.side_effect = Exception("connect fail")
    assert make_pg_engine() is None

@patch("utils.load.text")
def test_ensure_products_table_ok(mock_text):
    engine = MagicMock()
    ctx = MagicMock()
    ctx.__enter__.return_value = MagicMock()
    engine.begin.return_value = ctx
    ensure_products_table(engine)
    engine.begin.assert_called_once()

def test_ensure_products_table_none_engine():
    with pytest.raises(RuntimeError):
        ensure_products_table(None)

@patch("utils.load.text")
def test_ensure_products_table_ddl_error(mock_text):
    engine = MagicMock()
    ctx = MagicMock()
    conn = MagicMock()
    ctx.__enter__.return_value = conn
    engine.begin.return_value = ctx
    conn.execute.side_effect = Exception("ddl")
    with pytest.raises(Exception):
        ensure_products_table(engine)

@patch("utils.load.ensure_products_table")
@patch("utils.load.make_pg_engine")
@patch("utils.load.text")
def test_save_postgres_insert_ok(mock_text, mock_engine_mk, mock_ensure, df_pg_ok):
    engine = MagicMock()
    ctx = MagicMock()
    conn = MagicMock()
    ctx.__enter__.return_value = conn
    engine.begin.return_value = ctx
    mock_engine_mk.return_value = engine
    assert save_postgres(df_pg_ok) == len(df_pg_ok)
    mock_engine_mk.assert_called_once()
    mock_ensure.assert_called_once_with(engine)
    engine.begin.assert_called_once()
    assert conn.execute.call_count == 1

@patch("utils.load.ensure_products_table")
@patch("utils.load.make_pg_engine")
@patch("utils.load.text")
def test_save_postgres_engine_none_short_circuit(mock_text, mock_engine_mk, mock_ensure, df_pg_ok):
    mock_engine_mk.return_value = None
    assert save_postgres(df_pg_ok) == 0
    mock_ensure.assert_not_called()

@patch("utils.load.ensure_products_table")
@patch("utils.load.make_pg_engine")
@patch("utils.load.text")
def test_save_postgres_object_price_cleanup(mock_text, mock_engine_mk, mock_ensure):
    df = pd.DataFrame({
        "Title": ["A","B"],
        "Price": ["12345.67","89000"],
        "Rating": ["4.5","3.0"],
        "Colors": ["2","3"],
        "Size": ["M","L"],
        "Gender": ["Men","Women"],
        "Timestamp": pd.to_datetime(["2025-01-01","2025-01-02"])
    })
    engine = MagicMock()
    ctx = MagicMock()
    conn = MagicMock()
    ctx.__enter__.return_value = conn
    engine.begin.return_value = ctx
    mock_engine_mk.return_value = engine
    assert save_postgres(df) == 2
    assert conn.execute.call_count == 1

@patch("utils.load.ensure_products_table")
@patch("utils.load.make_pg_engine")
@patch("utils.load.text")
def test_save_postgres_dollar_to_idr(mock_text, mock_engine_mk, mock_ensure):
    df = pd.DataFrame({
        "Title": ["A"],
        "Price": ["$10.00"],
        "Rating": [3.0],
        "Colors": [2],
        "Size": ["M"],
        "Gender": ["Men"],
        "Timestamp": pd.to_datetime(["2025-01-01"])
    })
    engine = MagicMock()
    ctx = MagicMock()
    conn = MagicMock()
    ctx.__enter__.return_value = conn
    engine.begin.return_value = ctx
    mock_engine_mk.return_value = engine
    assert save_postgres(df) == 1
    conn.execute.assert_called_once()

@patch("utils.load.ensure_products_table")
@patch("utils.load.make_pg_engine")
def test_save_postgres_dropna_all(mock_engine_mk, mock_ensure):
    df = pd.DataFrame({
        "Title": [None],
        "Price": [None],
        "Rating": [None],
        "Colors": [None],
        "Size": [None],
        "Gender": [None],
        "Timestamp": [pd.NaT]
    })
    mock_engine_mk.return_value = MagicMock()
    assert save_postgres(df) == 0
