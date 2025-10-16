import pandas as pd
import os
processed_dir = os.path.join('data', 'processed')
try:
    df = pd.read_parquet(processed_dir)
except Exception:
    import glob
    files = glob.glob(os.path.join(processed_dir, '*.csv'))
    if not files:
        raise RuntimeError('Processed data not found. Run preprocessing first.')
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
print('Rows:', len(df))
print(df.groupby('region')['num_transactions'].mean())
