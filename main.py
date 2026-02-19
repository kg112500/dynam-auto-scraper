import time
import re
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import os
import json
from datetime import datetime

# Google Sheets / Selenium 関連ライブラリ
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# --- ★設定: 以下のIDを書き換えてください ---
SPREADSHEET_KEY = "1SEDGQLHGRN0rnXgLvP7wNzUuch6oxs9W4AvsavTagKM"
# ----------------------------------------

LIST_URL = "https://min-repo.com/tag/%e3%83%80%e3%82%a4%e3%83%8a%e3%83%a0%e6%bb%8b%e8%b3%80%e5%bd%a6%e6%a0%b9%e5%ba%97/"
MAX_PAGES = 2

def setup_driver():
    options = webdriver.ChromeOptions()
    # GitHub Actions用の必須設定
    options.add_argument('--headless') 
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36')
    options.add_argument('--ignore-certificate-errors')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def add_kishu_param(url):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query['kishu'] = ['all']
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))

def extract_and_format_date(title_text):
    try:
        match_full = re.search(r'(\d{4})/(\d{1,2})/(\d{1,2})', title_text)
        if match_full:
            return f"{int(match_full.group(1))}/{int(match_full.group(2)):02}/{int(match_full.group(3)):02}"

        match_short = re.search(r'(\d{1,2})/(\d{1,2})', title_text)
        if match_short:
            month = int(match_short.group(1))
            day = int(match_short.group(2))
            if month > 12: month, day = day, month
            now = datetime.now()
            year = now.year - 1 if (now.month <= 3 and month >= 10) else now.year
            return f"{year}/{month:02}/{day:02}"
        return "日付不明"
    except: return "日付不明"

def get_data_via_js(driver):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)
    tables = driver.find_elements(By.TAG_NAME, "table")
    target_table = None
    for table in tables:
        if "機種" in driver.execute_script("return arguments[0].innerText;", table):
            target_table = table
            break
    if not target_table: return None

    js_script = """
        var rows = arguments[0].querySelectorAll('tr');
        var results = [];
        for (var i=0; i<rows.length; i++) {
            var row = [], cells = rows[i].querySelectorAll('td, th');
            for (var j=0; j<cells.length; j++) row.push(cells[j].textContent.replace(/\\n/g,'').trim());
            if (row.some(c=>c!=='')) results.push(row);
        }
        return results;
    """
    try:
        raw = driver.execute_script(js_script, target_table)
        if not raw or len(raw) < 2: return None
        return pd.DataFrame(raw[1:], columns=raw[0])
    except: return None

def update_google_sheet(new_df):
    print("\nGoogleスプレッドシートへ書き込みを開始します...")
    try:
        # 1. 認証とシートを開く
        key_json = os.environ.get('GCP_KEY_JSON')
        if not key_json: raise ValueError("環境変数 GCP_KEY_JSON が設定されていません")
        
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(key_json), ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        sh = client.open_by_key(SPREADSHEET_KEY)
        worksheet = sh.sheet1 

        # 2. 既存のデータを読み込み
        existing_data = worksheet.get_all_values()
        if existing_data:
            existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
        else:
            existing_df = pd.DataFrame()

        # 3. 新旧データを結合
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # 4. 重複削除
        if "日付" in combined_df.columns and "台番号" in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['日付', '台番号'], keep='last')
        else:
            combined_df = combined_df.drop_duplicates(keep='last')

        # 5. 日付でソート
        if "日付" in combined_df.columns:
            combined_df["日付"] = pd.to_datetime(combined_df["日付"], errors='coerce')
            combined_df = combined_df.sort_values("日付").dropna(subset=["日付"])
            # 日付を文字列に戻す (YYYY/MM/DD形式)
            combined_df["日付"] = combined_df["日付"].dt.strftime('%Y/%m/%d')

        # --- ★ここが重要: 数値カラムを正しく数値型(int)に変換する ---
        # これをやらないと、スプレッドシート側で文字列として扱われ ' が付きます
        numeric_cols = ["台番号", "総差枚", "差枚", "G数", "回転数"] # 変換したい列名
        for col in combined_df.columns:
            # 列名の一部に numeric_cols のいずれかが含まれていれば変換対象にする
            if any(target in col for target in numeric_cols):
                try:
                    # カンマ削除 -> 数値化 (エラーなら0) -> 整数化
                    combined_df[col] = (
                        combined_df[col]
                        .astype(str)
                        .str.replace(",", "")
                        .str.replace("+", "")
                        .str.strip()
                    )
                    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0).astype(int)
                except Exception as e:
                    print(f"Warning: {col} の数値変換に失敗しました ({e})")
        # -------------------------------------------------------

        # 6. 書き込み
        worksheet.clear()
        combined_df = combined_df.fillna("")
        
        # ★ここも重要: value_input_option='USER_ENTERED' を指定
        # これにより、文字列の日付もスプレッドシート側で自動的に日付形式として認識されます
        data_to_write = [combined_df.columns.values.tolist()] + combined_df.values.tolist()
        
        worksheet.update(
            range_name='A1', 
            values=data_to_write, 
            value_input_option='USER_ENTERED'
        )
        
        print(f"✅ 更新完了！ 合計データ数: {len(combined_df)} 件")
        
    except Exception as e:
        print(f"❌ スプレッドシート更新エラー: {e}")
def main():
    driver = setup_driver()
    try:
        driver.get(LIST_URL)
        time.sleep(5)
        links = []
        for a in driver.find_elements(By.TAG_NAME, "a"):
            href = a.get_attribute("href")
            if href and "min-repo.com" in href and re.search(r'/(\d{4,})/', href):
                links.append(href)
        
        target_links = list(dict.fromkeys(links))[:MAX_PAGES] # 重複排除してTop N件
        all_dfs = []

        for i, link in enumerate(target_links):
            driver.get(add_kishu_param(link))
            time.sleep(5)
            df = get_data_via_js(driver)
            if df is not None and not df.empty:
                df.insert(0, "日付", extract_and_format_date(driver.title))
                all_dfs.append(df)
                print(f"取得: {link}")

        if all_dfs:
            update_google_sheet(pd.concat(all_dfs, ignore_index=True))
        else:
            print("データなし")
    finally:
        driver.quit()

if __name__ == "__main__":

    main()















