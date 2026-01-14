# preprocessing.py
import pandas as pd
from pytz import timezone

def preprocess_workload(path_or_df, training=False):
    """
    Preprocess workload data.
    
    In training mode:
    - Expects raw event-level input with DURATION column.
    - Aggregates per second including sum of durations as duration_sum.
    - Returns 10 features used for training (excluding TIMESTAMP_last).
    
    In inference mode:
    - Expects raw event-level input without DURATION.
    - Aggregates per second and returns 10 features + TIMESTAMP_last.
    """
    print("DEBUG: preprocess_workload called with training =", training)
    # Load
    if isinstance(path_or_df, str):
        df = pd.read_csv(path_or_df, header=None)
    else:
        df = path_or_df.copy()

    # Auto-assign headers if missing based on column count
    if training:
        if 'TIMESTAMP' not in df.columns and df.shape[1] == 7:
            df.columns = ['TIMESTAMP', 'DY', 'HR', 'MN', 'OP', 'BYTES', 'DURATION']
            print("DEBUG: Assigned training headers to headerless CSV")
    else:
        if 'TIMESTAMP' not in df.columns and df.shape[1] == 6:
            df.columns = ['TIMESTAMP', 'DY', 'HR', 'MN', 'OP', 'BYTES']
            print("DEBUG: Assigned inference headers to headerless CSV")
    print("DEBUG: Columns in dataframe:", df.columns.tolist())
    # Ensure datetime index
    df['datetime'] = (
        pd.to_datetime(df['TIMESTAMP'], unit='s')
          .dt.tz_localize('UTC')
          .dt.tz_convert(timezone('US/Central'))
    )
    df.set_index('datetime', inplace=True)

    # Define aggregator based on mode
    def aggregator(chunk):
        if chunk.empty:
            base = {
                'bytes_op0': 0, 'bytes_op1': 0,
                'bytes_sum': 0,
                'io_count': 0,
                'read_ops_count': 0,
                'write_ops_count': 0
            }
            if training:
                base['duration_sum'] = 0
            else:
                base['TIMESTAMP_last'] = None
            return pd.Series(base)

        b0 = chunk.loc[chunk.OP == 0, 'BYTES'].sum()
        b1 = chunk.loc[chunk.OP == 1, 'BYTES'].sum()
        
        base = {
            'bytes_op0': b0,
            'bytes_op1': b1,
            'bytes_sum': chunk.BYTES.sum(),
            'io_count': len(chunk),
            'read_ops_count': (chunk.OP == 0).sum(),
            'write_ops_count': (chunk.OP == 1).sum()
        }

        if training:
            base['duration_sum'] = chunk['DURATION'].sum()
        else:
            base['TIMESTAMP_last'] = chunk['TIMESTAMP'].iloc[-1]

        return pd.Series(base)

    # Resample and aggregate
    agg = df.resample('1s').apply(aggregator).reset_index()
    agg = agg.dropna(subset=['bytes_sum'])
    agg = agg[agg["io_count"] > 0].reset_index(drop=True)

    # Compute EMAs and MACD
    short, long, signal = 10, 60, 20
    agg['bytes_sum_ema_short']   = agg.bytes_sum.ewm(span=short,  adjust=False).mean()
    agg['bytes_sum_ema_long']    = agg.bytes_sum.ewm(span=long,   adjust=False).mean()
    agg['bytes_sum_macd']        = agg.bytes_sum_ema_short - agg.bytes_sum_ema_long
    agg['bytes_sum_macd_signal'] = agg.bytes_sum_macd.ewm(span=signal, adjust=False).mean()

    # Output
    if training:
        return agg[[
            'bytes_op0','bytes_op1','bytes_sum',
            'duration_sum',
            'io_count','read_ops_count','write_ops_count',
            'bytes_sum_ema_short','bytes_sum_ema_long',
            'bytes_sum_macd','bytes_sum_macd_signal'
        ]]
    else:
        return agg[[
            'TIMESTAMP_last',
            'bytes_op0','bytes_op1','bytes_sum',
            'io_count','read_ops_count','write_ops_count',
            'bytes_sum_ema_short','bytes_sum_ema_long',
            'bytes_sum_macd','bytes_sum_macd_signal'
        ]]