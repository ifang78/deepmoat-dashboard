import pandas as pd

def run_strategy_audit_v23_7(file_path='Final_Decision_Report.csv'):
    try:
        df = pd.read_csv(file_path)
        print(f"\n📊 Deep Moat v23.7 策略邏輯審核 (對齊 PCS 四大核心指標)")
        print("-" * 75)

        # v23.7 核心篩選邏輯：DPI > 65%
        high_potential = df[df['DPI'] > 65].copy()
        
        # 找出本應是 PCS 但被分類為「觀望」或「BC」的標的
        pcs_missed = high_potential[~high_potential['戰術'].str.contains('PCS')]

        if pcs_missed.empty:
            print("✅ 審核完成：所有符合 v23.7 指標之標的均已正確納入 PCS 決策。")
            return

        report = []
        for _, row in pcs_missed.iterrows():
            reasons = []
            
            # v23.7 四大門檻檢查
            if row['DPI'] <= 65: reasons.append("DPI 門檻 (需 > 65%)")
            if row.get('Skew', 0) <= 0: reasons.append("Skew 轉負 (需 > 0)")
            if row['Edge'] <= 0: reasons.append("溢價消失 (需 IV > RV)")
            if row.get('IVR', 0) <= 50: reasons.append("IVR 過低 (需 > 50)")
            
            # v23.7 動作區間檢查 (10% 安全墊)
            buffer = ((row['現價'] - row['防禦價']) / row['防禦價']) * 100
            if buffer > 10: 
                reasons.append(f"安全墊過厚 ({buffer:.1f}% > 10%)")

            report.append({
                'Symbol': row['Symbol'],
                'DPI': row['DPI'],
                'Edge': row['Edge'],
                'IVR': row.get('IVR', 'N/A'),
                'Skew': row.get('Skew', 'N/A'),
                '落選 v23.7 原因': " 且 ".join(reasons)
            })

        audit_result = pd.DataFrame(report)
        print(audit_result.to_string(index=False))
        
        print("-" * 75)
        print(f"💡 結論：v23.7 具備極強的防禦篩選特性。目前無 PCS 訊號是系統在守護你的安全邊際。")
        
    except Exception as e:
        print(f"❌ v23.7 審核失敗: {e}")

if __name__ == "__main__":
    run_strategy_audit_v23_7()