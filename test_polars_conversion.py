#!/usr/bin/env python3
"""
Test Polars DataFrame conversion to C++.
"""

import polars as pl
import numpy as np
from datetime import datetime, timezone, timedelta

# Create a simple test DataFrame
base_time = datetime.now(timezone.utc)
df = pl.DataFrame({
    'trade_id': [1, 2, 3],
    'price': [50000.0, 50100.0, 49900.0],
    'quantity': [0.1, 0.2, 0.15],
    'is_buy': [True, False, True],
    'datetime': [base_time, base_time + timedelta(seconds=1), base_time + timedelta(seconds=2)]
})

print("DataFrame:")
print(df)
print(f"DataFrame type: {type(df)}")

# Test direct access to columns
print("\nColumn access:")
print(f"trade_id column: {df['trade_id']}")
print(f"trade_id type: {type(df['trade_id'])}")

# Test conversion to numpy arrays
print("\nNumpy conversion:")
trade_ids = df['trade_id'].to_numpy()
prices = df['price'].to_numpy()
print(f"trade_ids: {trade_ids}")
print(f"trade_ids type: {type(trade_ids)}")
print(f"prices: {prices}")
print(f"prices type: {type(prices)}")

# Test datetime conversion
print("\nDatetime conversion:")
datetimes = df['datetime'].to_numpy()
print(f"datetimes: {datetimes}")
print(f"datetimes type: {type(datetimes)}")

# Convert to milliseconds
datetime_ms = df['datetime'].cast(pl.Int64) // 1000
print(f"datetime_ms: {datetime_ms}")
print(f"datetime_ms type: {type(datetime_ms)}")
