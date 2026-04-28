import pandas as pd
import yfinance as yf
import numpy as np
from datetime import datetime, timedelta
import requests
import time
import glob
import os
import subprocess

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
    """v23.7 終極同步：確保雲端看板與 Mac Mini 數據 100% 一致"""
    print("📡 正在強制同步數據至 GitHub...")
    try:
        # 1. 確保所有變動都進入暫存區
        subprocess.run(["git", "add", "."], check=True)
        
        # 2. 提交變更 (即便數據微調也會強制提交)
        commit_msg = f"v23.7 Dashboard Auto-sync: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", commit_msg, "--allow-empty"], check=True)

        # 3. 強力推送 (使用 -f 確保覆蓋舊網址留下的任何衝突)
        # 剛才 git remote set-url 已經修正，這步會直接通往 deepmoat-dashboard
        subprocess.run(["git", "push", "-u", "origin", "main", "--force"], check=True)
        
        print("🚀 GitHub 數據同步成功！看板已更新。")
        return True
    except Exception as e:
        print(f"❌ 同步失敗: {e}")
        return False

# ==========================================
# 🛠️ 第二區：核心邏輯與位階函數 (v23.7)
# ==========================================
def get_upward_label(rise_pct):
    if rise_pct >= 0.20: return "牛勢"
    if rise_pct >= 0.10: return "逆轉"
    if rise_pct >= 0.05: return "反彈"
    if rise_pct >= 0: return "止跌回升"
    return "新低"

def calculate_rv(price_data):
    log_return = np.log(price_data / price_data.shift(1))
    return log_return.std() * np.sqrt(252) * 100

def force_clean(series):
    clean_s = series.astype(str).str.replace(r"[',%$]", "", regex=True)
    nums = pd.to_numeric(clean_s, errors='coerce').fillna(0)
    if 0 < nums.max() <= 1.0: return nums * 100
    return nums

# ==========================================
# 🚀 第三區：主執行引擎
# ==========================================
def run_v23_7_engine():
    print(f"🏰 Deep Moat v23.7：策略引擎啟動 (Dual DPI Mode)...")

    # 1. 定位最新數據
    csv_files = glob.glob('SPX_data-table_*.csv')
    if not csv_files: return
    latest_csv = max(csv_files, key=os.path.getmtime)
    df = pd.read_csv(latest_csv, header=[0, 1])

    # 2. 欄位定義 (已精確校準為 '5 day DPI')
    sym_col, pri_col = ('Unnamed: 1_level_0', 'Symbol'), ('Ticker Information', 'Current Price')
    kgs_col, dpi_col = ('SpotGamma Key Daily Levels', 'Key Gamma Strike'), ('Dark Pool Indicators', 'DPI')
    dpi5_col = ('Dark Pool Indicators', '5 day DPI') 
    skew_col, hw_col = ('Volatility Insights', 'Skew'), ('SpotGamma Key Daily Levels', 'Hedge Wall')
    pw_col, cw_col = ('SpotGamma Key Daily Levels', 'Put Wall'), ('SpotGamma Key Daily Levels', 'Call Wall')
    iv_col, ivr_col = ('Volatility Insights', '1 M IV'), ('Volatility Insights', 'IV Rank')
    imp_col = ('SpotGamma Key Daily Levels', 'Options Impact')
    earn_col = ('Ticker Information', 'Earnings Date')

    # 3. 數據清洗
    print("🧹 清洗標度與去除雜訊...")
    clean_list = [pri_col, kgs_col, dpi_col, dpi5_col, skew_col, hw_col, pw_col, cw_col, imp_col, iv_col, ivr_col]
    for c in clean_list:
        if c in df.columns: df[c] = force_clean(df[c])

    # 4. 海選 (DPI > 55% 即可進入初選，包含獵人標的)
    mask = (df[dpi_col] > 55)
    candidates_df = df[mask].copy()
    
    tickers = candidates_df[sym_col].dropna().unique().tolist()
    if not tickers:
        send_telegram_msg("📢 今日無符合 DPI > 55% 之標的。")
        return

    print(f"📡 鎖定 {len(tickers)} 隻候選標的。執行深度審計...")
    
    final_results = []
    report_msg = f"🏆 **Deep Moat 決策報告 (v23.7)**\n"
    found_count = 0

    for i in range(0, len(tickers), 20):
        batch = tickers[i:i+20]
        data = yf.download(batch, period="1y", threads=False, progress=False)
        
        for ticker in batch:
            try:
                row = candidates_df[candidates_df[sym_col] == ticker].iloc[0]
                p = row[pri_col]
                hist = data.xs(ticker, axis=1, level=1).dropna() if len(batch) > 1 else data.dropna()
                if len(hist) < 251: continue
                
                l_61 = hist.tail(61)['Low'].min()
                l_61_up = get_upward_label((hist['High'].iloc[-1] - l_61) / l_61)
                real_rv = calculate_rv(hist['Close'])
                edge = row[iv_col] - real_rv
                
                buffer = (p - row[pw_col]) / p if p != 0 else 0
                slingshot = (p - row[kgs_col]) / row[kgs_col] if row[kgs_col] != 0 else 0
                vacuum = (row[cw_col] - p) / p if p != 0 else 0

                # ==========================================
                # ⚖️ v23.7 核心戰術決策
                # ==========================================
                action = "⏳ 觀望"
                
                # PCS：當日強(>65) 且 5日穩(>55) 且 具備租金溢價
                if (row[dpi_col] > 65 and row[dpi5_col] > 55 and row[skew_col] > 0 and 
                    edge > 0 and row[ivr_col] > 50 and l_61_up == "止跌回升" and 
                    buffer >= 0.05 and buffer <= 0.12):
                    action = "🏡 穩健地主 (PCS)"
                
                # BC：當日達標(>55) 且 動能極強
                elif (row[dpi_col] > 55 and slingshot >= 0.05 and row[imp_col] > 15 and 
                      vacuum > 0.15 and row[skew_col] > 0 and edge < 0 and 
                      l_61_up in ["牛勢", "新高"]):
                    action = "🎯 進攻獵人 (BC)"

                final_results.append({
                    "Symbol": ticker, "現價": round(p, 2), "DPI": round(row[dpi_col], 2), 
                    "5D_DPI": round(row[dpi5_col], 2), "Edge": round(edge, 2), 
                    "防禦價": round(row[pw_col], 2), "位階": l_61_up, "戰術": action, "板塊": "待定"
                })

                if action != "⏳ 觀望":
                    found_count += 1
                    report_msg += f"🔥 **{ticker}** ({action})\nDPI: {row[dpi_col]:.0f}% | 5D: {row[dpi5_col]:.0f}%\n"
            except: continue
        
        time.sleep(0.5)

    # 數據落地
    pd.DataFrame(final_results).to_csv('Final_Decision_Report.csv', index=False)
    
    # 同步雲端
    sync_status = auto_sync_to_github()

    # 發送通報
    if found_count > 0:
        sync_text = "✅ 雲端已更新" if sync_status else "❌ 同步失敗"
        send_telegram_msg(report_msg + f"\n系統狀態: {sync_text}\n👉 [點此查看最新看板](https://deepmoat-dashboard.streamlit.app/)")
    else:
        send_telegram_msg("📢 今日 v23.7 掃描完畢，未發現精英信號。")
    
    print(f"🎉 全部流程執行完畢。共找到 {found_count} 個信號。")

if __name__ == "__main__":
    run_v23_7_engine()
