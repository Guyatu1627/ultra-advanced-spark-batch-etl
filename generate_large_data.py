import pandas as pd
import numpy as np
from pathlib import Path

Path("data/raw").mkdir(parents=True, exist_ok=True)

np.random.seed(42)
n_rows = 5_000_000  # 5M rows – realistic scale for portfolio

df = pd.DataFrame({
    'transaction_id': range(1, n_rows + 1),
    'user_id': np.random.randint(1, 100_000, n_rows),
    'product_id': np.random.randint(1, 20_000, n_rows),
    'category': np.random.choice(['Electronics', 'Fashion', 'Books', 'Home', 'Sports', 'Beauty', 'Grocery'], n_rows),
    'price': np.random.lognormal(mean=4, sigma=1.2, size=n_rows).round(2),  # skewed prices
    'quantity': np.random.randint(1, 20, n_rows),
    'timestamp': pd.date_range('2023-01-01', periods=n_rows, freq='30s')[:n_rows],
    'country': np.random.choice(['US', 'UK', 'DE', 'FR', 'IN', 'CA', 'AU'], n_rows),
    'discount_code': np.random.choice([None, 'WINTER15', 'FLASH30', 'NEWUSER'], n_rows, p=[0.65, 0.15, 0.15, 0.05]),
})

# Real-world mess: missing prices, extreme skew, duplicates
df.loc[df['user_id'] % 30 == 0, 'price'] = np.nan
df.loc[df['user_id'] % 500 == 0, 'quantity'] *= 50  # heavy users

df.to_csv("data/raw/transactions_5M.csv", index=False)
print("Generated 5M row realistic dataset: data/raw/transactions_5M.csv")