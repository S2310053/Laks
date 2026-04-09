import sys, traceback
try:
    import pandas as pd, numpy as np

    BASE = 'C:/Users/arzol/OneDrive/Escritorio/Work/Master Thesis/Laks/'

    xl = pd.read_excel(BASE + 'Data/Salmon/Market/Forward_Prices_Bloomberg.xlsx', sheet_name=None, header=0)
    print('BBG Sheets:', list(xl.keys()))
    for sheet, df in xl.items():
        vals = pd.to_numeric(df['Last Price'], errors='coerce')
        print(f'\nSheet={sheet} | n={len(df)} | min={vals.min():.4f} | max={vals.max():.4f} | mean={vals.mean():.4f}')
        print(df.head(8).to_string())

    print('\n--- Old FishPool CSV (first 5 rows) ---')
    df_old = pd.read_csv(BASE + 'Data/Salmon/Market/Forwardprices_20062024.csv', sep=';', skiprows=1, decimal=',', nrows=5)
    print(df_old.to_string())

    print('\n--- New FishPool CSV (first 5 rows) ---')
    df_new = pd.read_csv(BASE + 'Data/Salmon/Market/Forwardprices_20252026.csv', sep=';', skiprows=1, decimal=',', nrows=5)
    print(df_new.to_string())

except Exception:
    traceback.print_exc()
    sys.exit(1)
