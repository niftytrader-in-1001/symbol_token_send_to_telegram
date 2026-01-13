import requests
import pandas as pd
from datetime import datetime
import os
import zipfile
from io import BytesIO, StringIO

# ---------------- CONFIG ----------------
NFO_URL = "https://api.shoonya.com/NFO_symbols.txt.zip"
BFO_URL = "https://api.shoonya.com/BFO_symbols.txt.zip"

TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CHAT_ID")

# True  -> keep Excel file
# False -> delete Excel file after sending
SAVE_FILE_LOCALLY = False
# ----------------------------------------

def detect_separator(sample: str):
    """
    Detect whether CSV/TXT uses comma or tab as separator.
    """
    if sample.count("\t") > sample.count(","):
        return "\t"
    return ","

def download_and_extract_zip(url):
    """
    Download a ZIP file, detect separator, and return its content as a DataFrame.
    Assumes the ZIP contains a single text file.
    """
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with zipfile.ZipFile(BytesIO(response.content)) as z:
        file_name = z.namelist()[0]
        with z.open(file_name) as f:
            # Read a small sample to detect separator
            sample_bytes = f.read(1024)  # first 1KB
            sample_text = sample_bytes.decode("utf-8", errors="ignore")
            sep = detect_separator(sample_text)

            # Reset pointer to start
            f.seek(0)
            df = pd.read_csv(f, sep=sep)
    return df

def sort_df(df):
    # Only attempt to sort if these columns exist
    if "strike" in df.columns:
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    if "expiry" in df.columns:
        df["expiry_sort"] = pd.to_datetime(df["expiry"], errors="coerce")
        df = df.sort_values(by=["strike", "expiry_sort"], ascending=[True, True])
        df = df.drop(columns=["expiry_sort"])
    return df

def create_excel(nfo_df, bfo_df):
    current_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"symbol_token_{current_date}.xlsx"

    # Apply sorting
    nfo_df = sort_df(nfo_df)
    bfo_df = sort_df(bfo_df)

    with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
        nfo_df.to_excel(writer, sheet_name="NFO", index=False)
        bfo_df.to_excel(writer, sheet_name="BFO", index=False)

    return file_name

def send_to_telegram(file_path):
    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    with open(file_path, "rb") as f:
        files = {"document": f}
        data = {"chat_id": TELEGRAM_CHAT_ID}
        response = requests.post(telegram_url, files=files, data=data)
    response.raise_for_status()

def main():
    print("📥 Downloading and extracting NFO symbols...")
    nfo_df = download_and_extract_zip(NFO_URL)

    print("📥 Downloading and extracting BFO symbols...")
    bfo_df = download_and_extract_zip(BFO_URL)

    print("📊 Creating Excel (NFO & BFO sheets)...")
    excel_file = create_excel(nfo_df, bfo_df)

    print("📤 Sending file to Telegram...")
    send_to_telegram(excel_file)

    if not SAVE_FILE_LOCALLY and os.path.exists(excel_file):
        os.remove(excel_file)
        print("🧹 Local file deleted")

    print(f"✅ Done! File sent: {excel_file}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Error occurred:", e)
