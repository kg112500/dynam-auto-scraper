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
MAX_PAGES = 3

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

        # 2. 既存のデータをすべて読み込む
        existing_data = worksheet.get_all_values()
        
        # 既存データがある場合、DataFrameに変換
        if existing_data:
            headers = existing_data[0]
            existing_df = pd.DataFrame(existing_data[1:], columns=headers)
            print(f"既存データ: {len(existing_df)} 件を読み込みました。")
        else:
            existing_df = pd.DataFrame()
            print("既存データはありません。")

        # 3. 新旧データを結合 (Concat)
        # カラム名が一致しているか確認しつつ結合
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # 4. 重複を削除 (日付と台番号が同じなら、重複とみなして1つ残す)
        # ※もし「台番号」列がない場合は、全列を見て重複削除します
        if "日付" in combined_df.columns and "台番号" in combined_df.columns:
            before_len = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=['日付', '台番号'], keep='last')
            print(f"重複削除: {before_len - len(combined_df)} 件をカットしました。")
        else:
            combined_df = combined_df.drop_duplicates(keep='last')

        # 5. 日付順に並び替え (オプション: 新しい日付が下に来るように)
        if "日付" in combined_df.columns:
            combined_df["日付"] = pd.to_datetime(combined_df["日付"], errors='coerce')
            combined_df = combined_df.sort_values("日付").dropna(subset=["日付"])
            combined_df["日付"] = combined_df["日付"].dt.strftime('%Y/%m/%d') # 文字列に戻す

        # 6. 書き込み (一度クリアしてから全データを書き戻す)
        worksheet.clear()
        
        # NaNを空文字に変換
        combined_df = combined_df.fillna("")
        
        # データ書き込み
        worksheet.update([combined_df.columns.values.tolist()] + combined_df.values.tolist())
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




