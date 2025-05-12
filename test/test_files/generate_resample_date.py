import pandas as pd
import os

# File paths
input_csv = os.path.join(os.path.dirname(__file__), 'EURUSD_15M.csv')

# Read CSV with datetime index
df = pd.read_csv(input_csv, parse_dates=['Date'], index_col='Date')

# Timeframes and their pandas offset aliases
TIMEFRAMES = {
    '15min': '15T',
    '2hr': '2H',
    '1D': '1D',
    '1W': '1W',
    '1ME': 'M',
    '1Quarter': 'Q',
    '1Year': 'A',
}

# Resample combinations
LABELS = ['right', 'left']
CLOSEDS = ['right', 'left']

# All valid pandas origins (including None for default)
ORIGINS = [
    None,           # Default origin
    'epoch',
    'start',
    'start_day',
    'end',
    'end_day',
    '2000-01-01',  # Example specific timestamp
]

# How to aggregate OHLCV data
agg_dict = {
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last',
    'Tickvol': 'sum',
    'Volume': 'sum',
    'Spread': 'mean',
}
resampled = df.resample(rule="w", label="right", closed="right", origin="start").agg(agg_dict).dropna()

# for tf_name, tf_rule in TIMEFRAMES.items():
#     for label in LABELS:
#         for closed in CLOSEDS:
#             for origin in ORIGINS:
#                 try:
#                     kwargs = dict(
#                         rule=tf_rule,
#                         label=label,
#                         closed=closed
#                     )
#                     if origin is not None:
#                         kwargs['origin'] = origin
#                     resampled = df.resample(**kwargs).agg(agg_dict).dropna()
#                     fname = f"EURUSD_{tf_name}_label-{label}_closed-{closed}_origin-{origin if origin is not None else 'default'}.csv"
#                     out_path = os.path.join(os.path.dirname(__file__), fname)
#                     resampled.to_csv(out_path)
#                     print(f"Saved: {fname}")
#                 except Exception as e:
#                     print(f"Failed: {tf_name}, {label}, {closed}, {origin} -- {e}")
