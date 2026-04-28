import pandas as pd
import yfinance as yf
import numpy as np
from datetime import datetime, timedelta
import requests
import time
import glob
import os
import subprocess  # 新增：用於執行系統指令

# ==========================================
# ⚙️ 第一區：連線與自動化設定
# ==========================================
TELEGRAM_TOKEN = "8594513111:AAFhT6-mHqEAXeks-w_YGKkIdslvehYy-P8"
TELEGRAM_CHAT_ID = "-1003955011272" 

def send_telegram_msg(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for i in range(0, len(message), 3500):
        chunk = message[i:i+3500]
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "Markdown"}
        try:
            requests.post(url, json=payload, timeout=15)
        except: pass
        time.sleep(1.2)

def auto_sync_to_github():
    """自動將產出的 CSV 同步至 GitHub"""
    print("📡 正在將最新報告自動同步至 GitHub...")
    try:
        # 1. 加入檔案
        subprocess.run(["git", "add", "Final_Decision_Report.csv"], check=True)
        # 2. 提交變更
        commit_msg = f"Auto-update report: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        # 3. 推送至遠端
        subprocess.run(["git", "push"], check=True)
        print("🚀 GitHub 同步成功！")
        return True
    except Exception as e:
        print(f"❌ Git 同步失敗: {e}")
        return False

# ==========================================
# 🛠️ 第二區：核心邏輯與位階函數 (100% 復刻 v24.10)
# ==========================================
def get_upward_label(rise_pct):
    if rise_pct >= 0.20: return "牛勢"
    if rise_pct >= 0.10: return "逆轉"
    if rise_pct >= 0.05: return "反彈"
    if rise_pct >= 0: return "止跌回升"
    return "新低"

def get_downward_label(drop_pct):
    if drop_pct <= -0.20: return "熊式"
    if drop_pct <= -0.10: return "修正"
    if drop_pct <= -0.05: return "拉回"
    if drop_pct <= 0: return "超賣"
    return "新高"

def calculate_rv(price_data):
    """計算年化真實波動率"""
    log_return = np.log(price_data / price_data.shift(1))
    return log_return.std() * np.sqrt(252) * 100

def force_clean(series):
    """校準標度與清洗格式 (0.85 -> 85) - 執行全欄位判斷"""
    clean_s = series.astype(str).str.replace(r"[',%$]", "", regex=True)
    nums = pd.to_numeric(clean_s, errors='coerce').fillna(0)
    if 0 < nums.max() <= 1.0: 
        return nums * 100
    return nums

# ==========================================
# 🚀 第三區：主執行引擎 (v24.35 自動同步版)
# ==========================================
def run_v24_35_sync_engine():
    print(f"🏰 Deep Moat v24.35：自動化流水線啟動...")

    # 1. 自動定位最新 CSV
    csv_files = glob.glob('SPX_data-table_*.csv')
    if not csv_files: 
        print("❌ 找不到數據源檔案")
        return
    latest_csv = max(csv_files, key=os.path.getmtime)
    print(f"📂 使用檔案: {latest_csv}")
    df = pd.read_csv(latest_csv, header=[0, 1])

    # 2. 定義欄位
    sym_col, pri_col = ('Unnamed: 1_level_0', 'Symbol'), ('Ticker Information', 'Current Price')
    kgs_col, dpi_col = ('SpotGamma Key Daily Levels', 'Key Gamma Strike'), ('Dark Pool Indicators', 'DPI')
    skew_col, hw_col = ('Volatility Insights', 'Skew'), ('SpotGamma Key Daily Levels', 'Hedge Wall')
    pw_col, cw_col = ('SpotGamma Key Daily Levels', 'Put Wall'), ('SpotGamma Key Daily Levels', 'Call Wall')
    iv_col, ivr_col = ('Volatility Insights', '1 M IV'), ('Volatility Insights', 'IV Rank')
    imp_col = ('SpotGamma Key Daily Levels', 'Options Impact')
    earn_col = ('Ticker Information', 'Earnings Date')

    # 3. 數據全欄位清洗
    print("🧹 正在同步數據標度...")
    clean_list = [pri_col, kgs_col, dpi_col, skew_col, hw_col, pw_col, cw_col, imp_col, iv_col, ivr_col]
    for c in clean_list:
        df[c] = force_clean(df[c])

    # 4. 板塊快取處理
    cache_file = 'sector_cache.csv'
    sector_cache = {}
    if os.path.exists(cache_file):
        c_df = pd.read_csv(cache_file)
        sector_cache = dict(zip(c_df['Symbol'], c_df['Sector']))

    # 5. 財報過濾
    def is_earn_safe(val):
        if pd.isna(val) or val in [0, '0', '']: return True
        try:
            dt = pd.to_datetime(val)
            if datetime.now() < dt < (datetime.now() + timedelta(hours=48)): return False
        except: pass
        return True

    # 6. 物理海選
    mask = (df[pri_col] >= df[kgs_col]) & (df[dpi_col] >= 50)
    candidates_df = df[mask].copy()
    candidates_df = candidates_df[candidates_df[earn_col].apply(is_earn_safe)]
    
    tickers = candidates_df[sym_col].dropna().unique().tolist()
    if not tickers:
        print("📢 無符合初步條件標的。")
        return

    print(f"📡 鎖定 {len(tickers)} 隻標的。正在進行批次審計...")
    
    final_results = []
    report_msg = f"🏆 **Deep Moat 決策報告 (v24.35)**\n"
    found_count = 0

    # 7. 批次處理
    for i in range(0, len(tickers), 20):
        batch = tickers[i:i+20]
        data = yf.download(batch, period="1y", threads=False, progress=False)
        
        for ticker in batch:
            try:
                row = candidates_df[candidates_df[sym_col] == ticker].iloc[0]
                p = row[pri_col]
                
                if ticker in sector_cache: sector = sector_cache[ticker]
                else:
                    s_info = yf.Ticker(ticker).info.get('sector', '其他')
                    s_map = {'Technology':'科技','Energy':'能源','Financial Services':'金融','Healthcare':'醫療','Utilities':'公用事業','Industrials':'工業','Consumer Cyclical':'消費','Communication Services':'通訊'}
                    sector = s_map.get(s_info, s_info)
                    sector_cache[ticker] = sector

                hist = data.xs(ticker, axis=1, level=1).dropna() if len(batch) > 1 else data.dropna()
                if len(hist) < 251: continue
                
                h_251 = hist['High'].max()
                l_61 = hist.tail(61)['Low'].min()
                l_61_up = get_upward_label((hist['High'].iloc[-1] - l_61) / l_61)
                l_251_dn = get_downward_label((hist['Low'].iloc[-1] - h_251) / h_251)
                
                real_rv = calculate_rv(hist['Close'])
                iv_rv_spread = row[iv_col] - real_rv
                
                slingshot = (p - row[kgs_col]) / row[kgs_col] if row[kgs_col] != 0 else 0
                buffer = (p - row[pw_col]) / p if p != 0 else 0
                vacuum = (row[cw_col] - p) / p if p != 0 else 0

                action = "⏳ 觀望"
                if (row[dpi_col] > 65 and row[skew_col] > 0 and iv_rv_spread > 0 and row[ivr_col] > 50 and 
                    l_61_up == "止跌回升" and l_251_dn != "熊式" and buffer >= 0.06 and row[hw_col] > row[pw_col]):
                    action = "🏡 穩健地主 (PCS)"
                elif (slingshot >= 0.05 and row[imp_col] > 15 and vacuum > 0.20 and row[skew_col] > 0 and 
                      iv_rv_spread < 0 and row[ivr_col] < 60 and l_61_up in ["牛勢", "新高"]):
                    action = "🎯 進攻獵人 (BC)"

                final_results.append({
                    "Symbol": ticker, "現價": round(p, 2), "DPI": round(row[dpi_col], 2), 
                    "Edge": round(iv_rv_spread, 2), "防禦價": round(row[kgs_col], 2), 
                    "位階": l_61_up, "戰術": action, "板塊": sector
                })

                if action != "⏳ 觀望":
                    found_count += 1
                    report_msg += f"🔥 **{ticker}** ({action})\nDPI: {row[dpi_col]:.0f}% | 位階: {l_61_up}\n"
            except: continue
        
        print(f"⌛️ 處理進度: {min(i+20, len(tickers))} / {len(tickers)}")
        time.sleep(1)

    # 8. 儲存 CSV
    pd.DataFrame(list(sector_cache.items()), columns=['Symbol', 'Sector']).to_csv(cache_file, index=False)
    pd.DataFrame(final_results).to_csv('Final_Decision_Report.csv', index=False)
    print("✅ 本地報告生成完畢。")

    # 9. 自動同步 GitHub
    sync_status = auto_sync_to_github()
    if sync_status:
        print("⏳ 等待雲端渲染中 (預留 15 秒)...")
        time.sleep(15) # 確保 Streamlit 有時間感應到檔案變更

    # 10. 發送推播
    if found_count > 0:
        send_telegram_msg(report_msg + f"\n👉 [點此查看最新看板](https://deepmoat-dashboard.streamlit.app/)")
    else:
        send_telegram_msg("📢 今日掃描完畢，未發現精英標的。")
    
    print(f"🎉 全部流程執行完畢。共找到 {found_count} 個信號。")

# --- 啟動入口 ---
if __name__ == "__main__":
    run_v24_35_sync_engine()