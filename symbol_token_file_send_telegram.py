import requests
import pandas as pd
from datetime import datetime
import os

# ---------------- CONFIG ----------------
JSON_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("CHAT_ID")


# True  -> keep Excel file
# False -> delete Excel file after sending
SAVE_FILE_LOCALLY = False
# ----------------------------------------


def download_json(url):
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def sort_df(df):
    # Convert strike to numeric for proper sorting
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")

    # Convert expiry to sortable datetime (handles empty values)
    df["expiry_sort"] = pd.to_datetime(df["expiry"], errors="coerce")

    # Sort by strike, then expiry
    df = df.sort_values(
        by=["strike", "expiry_sort"],
        ascending=[True, True]
    )

    # Drop helper column
    df = df.drop(columns=["expiry_sort"])

    return df


def json_to_excel(data):
    df = pd.DataFrame(data)

    # Split NFO & BFO
    df_nfo = df[df["exch_seg"] == "NFO"].copy()
    df_bfo = df[df["exch_seg"] == "BFO"].copy()

    # Apply sorting
    df_nfo = sort_df(df_nfo)
    df_bfo = sort_df(df_bfo)

    current_date = datetime.now().strftime("%Y-%m-%d")
    file_name = f"symbol_token_{current_date}.xlsx"

    # Write to single Excel with multiple sheets
    with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
        df_nfo.to_excel(writer, sheet_name="NFO", index=False)
        df_bfo.to_excel(writer, sheet_name="BFO", index=False)

    return file_name


def send_to_telegram(file_path):
    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"

    with open(file_path, "rb") as f:
        files = {"document": f}
        data = {"chat_id": TELEGRAM_CHAT_ID}
        response = requests.post(telegram_url, files=files, data=data)

    response.raise_for_status()


def main():
    print("📥 Downloading JSON...")
    json_data = download_json(JSON_URL)

    print("📊 Creating Excel (NFO & BFO sheets, sorted)...")
    excel_file = json_to_excel(json_data)

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
