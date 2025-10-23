import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError, SQLAlchemyError

def save_csv(df, path="products.csv"):
    try:
        df.to_csv(path, index=False)
        return path
    except Exception:
        return False

def save_google_sheets(df, name, creds_json):
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
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
        sh.share(None, perm_type="anyone", role="writer")

    data = df.copy()
    data["Timestamp"] = data["Timestamp"].astype(str)
    ws.update([data.columns.tolist()] + data.values.tolist())
    return sh.url

def make_pg_engine(host="localhost", port=5432, db="fashion", user="developer", password="supersecretpassword"):
    try:
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except OperationalError:
        return None
    except SQLAlchemyError:
        return None
    except Exception:
        return None

def ensure_products_table(engine):
    if engine is None:
        raise RuntimeError("Engine is None")
    ddl = """
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        price NUMERIC,
        rating NUMERIC,
        colors INTEGER,
        size TEXT,
        gender TEXT,
        ts TIMESTAMPTZ NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_products_title_ts ON products(title, ts);
    """
    try:
        with engine.begin() as conn:
            for stmt in ddl.strip().split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
    except ProgrammingError:
        raise
    except SQLAlchemyError:
        raise
    except Exception:
        raise

def save_postgres(df: pd.DataFrame, host="localhost", port=5432, db="fashion", user="developer", password="supersecretpassword") -> int:
    try:
        if df is None or df.empty:
            return 0
        engine = make_pg_engine(host, port, db, user, password)
        if engine is None:
            return 0
        ensure_products_table(engine)

        out = df.rename(columns={
            "Title":"title","Price":"price","Rating":"rating","Colors":"colors","Size":"size","Gender":"gender","Timestamp":"ts"
        })[["title","price","rating","colors","size","gender","ts"]].copy()

        if out["price"].dtype == object:
            out["price"] = (
                out["price"].astype(str).str.replace("$","",regex=False).str.replace(",","",regex=False)
            )
        out["price"] = pd.to_numeric(out["price"], errors="coerce")
        out["rating"] = pd.to_numeric(out["rating"], errors="coerce")
        out["colors"] = pd.to_numeric(out["colors"], errors="coerce").fillna(0).astype("Int64")
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce")

        before = len(out)
        out = out.dropna(subset=["price","ts","title"])
        if out.empty:
            return 0

        rows = out.to_dict(orient="records")
        sql = """
        INSERT INTO products (title, price, rating, colors, size, gender, ts)
        VALUES (:title, :price, :rating, :colors, :size, :gender, :ts)
        ON CONFLICT (title, ts) DO NOTHING;
        """
        with engine.begin() as conn:
            conn.execute(text(sql), rows)
        return len(rows)
    except OperationalError:
        raise
    except SQLAlchemyError:
        raise
    except Exception:
        raise
