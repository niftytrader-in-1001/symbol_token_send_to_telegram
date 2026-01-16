"""
expiry_symbol_dispatcher.py

Run DAILY.
Sends symbol files to Telegram ONLY on actual expiry day(s).
"""

import os
import io
import zipfile
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO

# ===================== TEST MODE =====================
FORCE_EXPIRY_TODAY = False   # 🔴 change to True for testing
# ====================================================
# ===================== CONFIG =====================
NFO_URL = "https://api.shoonya.com/NFO_symbols.txt.zip"
BFO_URL = "https://api.shoonya.com/BFO_symbols.txt.zip"

TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CHAT_ID")

TZ = "Asia/Kolkata"
REAL_TODAY = pd.Timestamp.now(tz=TZ).normalize()

TODAY = REAL_TODAY  # default

if FORCE_EXPIRY_TODAY:
    print("⚠️ TEST MODE ENABLED: Forcing expiry = today")

# Indices you care about
INDEX_CONFIG = [
    {"name": "NIFTY",      "symbol": "NIFTY",      "instrument": "OPTIDX", "exchange": "NFO", "type": "WEEKLY"},
    {"name": "BANKNIFTY",  "symbol": "BANKNIFTY",  "instrument": "OPTIDX", "exchange": "NFO", "type": "MONTHLY"},
    {"name": "FINNIFTY",   "symbol": "FINNIFTY",   "instrument": "OPTIDX", "exchange": "NFO", "type": "MONTHLY"},
    {"name": "MIDCPNIFTY","symbol": "MIDCPNIFTY","instrument": "OPTIDX", "exchange": "NFO", "type": "MONTHLY"},
    {"name": "SENSEX",     "symbol": "BSXOPT",     "instrument": "OPTIDX", "exchange": "BFO", "type": "WEEKLY"},
]
# =================================================



def download_symbol_master(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with zipfile.ZipFile(BytesIO(r.content)) as z:
        with z.open(z.namelist()[0]) as f:
            content = "\n".join(
                line.rstrip(",") for line in f.read().decode("utf-8").splitlines()
            )
            return pd.read_csv(io.StringIO(content))


def get_expiry_for_index(df, cfg):
    df = df[
        (df["Symbol"] == cfg["symbol"]) &
        (df["Instrument"] == cfg["instrument"])
    ].copy()

    if df.empty:
        return None, None

    df["Expiry_dt"] = pd.to_datetime(
        df["Expiry"], format="%d-%b-%Y", errors="coerce"
    ).dt.tz_localize(TZ)

    df = df.dropna(subset=["Expiry_dt"])

    if cfg["type"] == "WEEKLY":
        future = df[df["Expiry_dt"] >= TODAY]
        if future.empty:
            return None, None
        expiry = future["Expiry_dt"].min()

    else:  # MONTHLY
        future = df[df["Expiry_dt"] >= TODAY]
        if future.empty:
            return None, None
        expiry = future["Expiry_dt"].min()

    return expiry.normalize(), df


def build_expiry_files(df, cfg, expiry_dt):
    expiry_str = expiry_dt.strftime("%d-%b-%Y").upper()

    data = df[df["Expiry_dt"] == expiry_dt] \
        .drop(columns=["Expiry_dt"], errors="ignore")

    data = data.loc[:, ~data.columns.str.startswith("Unnamed")]

    filename = f"{cfg['name'].lower()}_{expiry_str}.txt"
    return filename, data.to_csv(index=False)


def send_zip_to_telegram(zip_bytes, zip_name):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    files = {"document": (zip_name, zip_bytes)}
    data = {"chat_id": TELEGRAM_CHAT_ID}
    r = requests.post(url, files=files, data=data, timeout=60)
    r.raise_for_status()


def main():
    print("📥 Loading symbol masters...")
    df_nfo = download_symbol_master(NFO_URL)
    df_bfo = download_symbol_master(BFO_URL)

    zip_buffer = io.BytesIO()
    added_files = 0
    zip_name = f"EXPIRY_SYMBOLS_{TODAY.strftime('%d-%b-%Y').upper()}.zip"

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for cfg in INDEX_CONFIG:
            df_src = df_nfo if cfg["exchange"] == "NFO" else df_bfo
            expiry_dt, df_idx = get_expiry_for_index(df_src, cfg)
        
            if expiry_dt is None:
                continue
        
            # 🚨 CORE CONDITION (works for PROD + TEST)
            if not FORCE_EXPIRY_TODAY and expiry_dt != TODAY:
                continue
        
            fname, content = build_expiry_files(df_idx, cfg, expiry_dt)
            zf.writestr(fname, content)
            added_files += 1
            print(f"✅ Expiry detected → {fname}")

    if added_files == 0:
        print("⏭ No expiry today. Exiting cleanly.")
        return

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Telegram credentials missing")

    zip_buffer.seek(0)
    send_zip_to_telegram(zip_buffer.read(), zip_name)
    print(f"📤 Sent {added_files} expiry files to Telegram")


if __name__ == "__main__":
    main()
