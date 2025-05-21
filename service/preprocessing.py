# preprocessing.py
import pandas as pd
from pytz import timezone

def preprocess_workload(path_or_df):
    """
    From raw events (6 columns: TIMESTAMP,DY,HR,MN,OP,BYTES)
    build exactly the 10 features for RF model was trained on:
      bytes_op0, bytes_op1, bytes_sum,
      io_count, read_ops_count, write_ops_count,
      bytes_sum_ema_short, bytes_sum_ema_long,
      bytes_sum_macd, bytes_sum_macd_signal
    """
    # 1) Load raw CSV (no DURATION)
    if isinstance(path_or_df, str):
        df = pd.read_csv(
            path_or_df, header=None,
            names=['TIMESTAMP','DY','HR','MN','OP','BYTES']
        )
    else:
        df = path_or_df.copy()

    # 2) Build datetime index in US/Central
    df['datetime'] = (
        pd.to_datetime(df['TIMESTAMP'], unit='s')
          .dt.tz_localize('UTC')
          .dt.tz_convert(timezone('US/Central'))
    )
    df.set_index('datetime', inplace=True)

    # 3) Aggregator: compute only the features needed
    def aggregator(chunk):
        if chunk.empty:
            return pd.Series({
                'bytes_op0': 0, 'bytes_op1': 0,
                'TIMESTAMP_last': None,
                'bytes_sum': 0,
                'io_count': 0,
                'read_ops_count': 0,
                'write_ops_count': 0
            })
        b0 = chunk.loc[chunk.OP==0, 'BYTES'].sum()
        b1 = chunk.loc[chunk.OP==1, 'BYTES'].sum()
        return pd.Series({
            'TIMESTAMP_last':   chunk['TIMESTAMP'].iloc[-1],
            'bytes_op0':       b0,
            'bytes_op1':       b1,
            'bytes_sum':       chunk.BYTES.sum(),
            'io_count':        len(chunk),
            'read_ops_count':  (chunk.OP==0).sum(),
            'write_ops_count': (chunk.OP==1).sum()
        })

    # 4) Resample & aggregate on 1-second bins
    agg = df.resample('1s').apply(aggregator).reset_index()
    # drop empty bins if any
    agg = agg.dropna(subset=['bytes_sum'])
    agg = agg[agg["io_count"] > 0].reset_index(drop=True)
    # 5) EMAs & MACD on bytes_sum
    short, long, signal = 10, 60, 20
    agg['bytes_sum_ema_short']   = agg.bytes_sum.ewm(span=short,  adjust=False).mean()
    agg['bytes_sum_ema_long']    = agg.bytes_sum.ewm(span=long,   adjust=False).mean()
    agg['bytes_sum_macd']        = agg.bytes_sum_ema_short - agg.bytes_sum_ema_long
    agg['bytes_sum_macd_signal'] = agg.bytes_sum_macd.ewm(span=signal, adjust=False).mean()

    # 6) Return exactly the 10 features (in the order RF model expects)
    return agg[[
        'TIMESTAMP_last',
        'bytes_op0','bytes_op1','bytes_sum',
        'io_count','read_ops_count','write_ops_count',
        'bytes_sum_ema_short','bytes_sum_ema_long',
        'bytes_sum_macd','bytes_sum_macd_signal'
    ]]
