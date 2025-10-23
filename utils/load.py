import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

def save_csv(df, path='products.csv'):
    try:
        df.to_csv(path, index=False)
        return path
    except Exception as e:
        return False

def save_google_sheets(df, name, creds_json):
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(creds_json, scopes=scopes)
        client = gspread.authorize(creds)
    except FileNotFoundError:
        raise
    except Exception:
        raise

    try:
        sh = client.open(name)
        ws = sh.sheet1
        ws.clear()
    except gspread.SpreadsheetNotFound:
        sh = client.create(name)
        ws = sh.sheet1
        # pastikan akun reviewer bisa mengakses (anyone writer)
        sh.share(None, perm_type='anyone', role='writer')
        
    data = df.copy()
    data['Timestamp'] = data['Timestamp'].astype(str)
    ws.update([data.columns.tolist()] + data.values.tolist())
    return sh.url
