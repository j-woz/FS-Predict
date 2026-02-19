# preprocessing.py
"""
Preprocessing utilities for FS-Predict with TFT.

This module converts raw event-level I/O logs into per-second aggregated rows.

Supports two modes:

1) training=True  (OBSERVE stream)
   Input raw columns (7): TIMESTAMP,DY,HR,MN,OP,BYTES,DURATION
   Output per-second rows include duration_sum (target) + covariates.

2) training=False (PREDICT stream)
   Input raw columns (6): TIMESTAMP,DY,HR,MN,OP,BYTES
   Output per-second rows include covariates (no duration_sum).

Notes:
- Aggregation uses timezone conversion to US/Central.
- Resample frequency is 1 second by default.
- Features include EMA/MACD computed from bytes_sum.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union

import pandas as pd
from pytz import timezone


CST = timezone("US/Central")


# columns expected for raw event-level data
RAW_COLS_INFER = ["TIMESTAMP", "DY", "HR", "MN", "OP", "BYTES"]
RAW_COLS_TRAIN = ["TIMESTAMP", "DY", "HR", "MN", "OP", "BYTES", "DURATION"]


# aggregated per-second columns
META_COLS = ["datetime", "TIMESTAMP_last", "DY_last", "HR_last", "MN_last"]
COVARIATE_COLS = [
    "bytes_op0",
    "bytes_op1",
    "bytes_sum",
    "io_count",
    "read_ops_count",
    "write_ops_count",
    "bytes_sum_ema_short",
    "bytes_sum_ema_long",
    "bytes_sum_macd",
    "bytes_sum_macd_signal",
]
TARGET_COL = "duration_sum"


@dataclass(frozen=True)
class EMAMACDConfig:
    short_span: int = 10
    long_span: int = 60
    signal_span: int = 20


def _load_raw(path_or_df: Union[str, pd.DataFrame], *, training: bool) -> pd.DataFrame:
    """Load a raw dataframe from a path or return a copy of a given dataframe."""
    if isinstance(path_or_df, str):
        df = pd.read_csv(path_or_df, header=None)
    else:
        df = path_or_df.copy()

    # Assign headers if needed
    if "TIMESTAMP" not in df.columns:
        if training:
            if df.shape[1] != 7:
                raise ValueError(f"Training raw data must have 7 cols, got {df.shape[1]}")
            df.columns = RAW_COLS_TRAIN
        else:
            if df.shape[1] != 6:
                raise ValueError(f"Inference raw data must have 6 cols, got {df.shape[1]}")
            df.columns = RAW_COLS_INFER

    # Minimal sanity checks
    needed = set(RAW_COLS_TRAIN if training else RAW_COLS_INFER)
    missing = needed.difference(df.columns)
    if missing:
        raise ValueError(f"Missing raw columns: {sorted(missing)}")

    return df


def _to_central_datetime_index(df: pd.DataFrame, *, tz=CST) -> pd.DataFrame:
    """Add tz-aware datetime index in US/Central from TIMESTAMP seconds."""
    out = df.copy()
    out["datetime"] = pd.to_datetime(out["TIMESTAMP"], unit="s")
    out["datetime"] = out["datetime"].dt.tz_localize("UTC").dt.tz_convert(tz)
    out = out.set_index("datetime")
    return out


def aggregate_raw_to_seconds(
    path_or_df: Union[str, pd.DataFrame],
    *,
    training: bool,
    tz=CST,
    freq: str = "1s",
    ema_macd: EMAMACDConfig = EMAMACDConfig(),
) -> pd.DataFrame:
    """
    Convert raw event-level rows -> per-second aggregated rows.

    Returns a DataFrame with:
      - datetime (tz-aware, US/Central)
      - TIMESTAMP_last, DY_last, HR_last, MN_last
      - covariates (COVARIATE_COLS)
      - duration_sum (only if training=True)

    Drops empty seconds (where no events occurred).
    """
    raw = _load_raw(path_or_df, training=training)
    df = _to_central_datetime_index(raw, tz=tz)

    def aggregator(subdf: pd.DataFrame) -> pd.Series:
        # If a second has no events, return defaults (will be dropped later)
        if subdf.empty:
            base = {
                "TIMESTAMP_last": None,
                "DY_last": None,
                "HR_last": None,
                "MN_last": None,
                "bytes_op0": 0,
                "bytes_op1": 0,
                "bytes_sum": 0,
                "io_count": 0,
                "read_ops_count": 0,
                "write_ops_count": 0,
            }
            if training:
                base["duration_sum"] = 0
            return pd.Series(base)

        bytes_op0 = subdf.loc[subdf["OP"] == 0, "BYTES"].sum()
        bytes_op1 = subdf.loc[subdf["OP"] == 1, "BYTES"].sum()
        bytes_sum = subdf["BYTES"].sum()

        io_count = len(subdf)
        read_ops_count = (subdf["OP"] == 0).sum()
        write_ops_count = (subdf["OP"] == 1).sum()

        out = {
            "TIMESTAMP_last": subdf["TIMESTAMP"].iloc[-1],
            "DY_last": subdf["DY"].iloc[-1],
            "HR_last": subdf["HR"].iloc[-1],
            "MN_last": subdf["MN"].iloc[-1],
            "bytes_op0": bytes_op0,
            "bytes_op1": bytes_op1,
            "bytes_sum": bytes_sum,
            "io_count": io_count,
            "read_ops_count": read_ops_count,
            "write_ops_count": write_ops_count,
        }

        if training:
            out["duration_sum"] = subdf["DURATION"].sum()

        return pd.Series(out)

    agg = df.resample(freq).apply(aggregator).reset_index()  # brings back 'datetime'
    agg = agg.dropna(subset=["TIMESTAMP_last"]).reset_index(drop=True)

    # EMA + MACD on bytes_sum (same as your notebook)
    s = ema_macd.short_span
    l = ema_macd.long_span
    sig = ema_macd.signal_span

    agg["bytes_sum_ema_short"] = agg["bytes_sum"].ewm(span=s, adjust=False).mean()
    agg["bytes_sum_ema_long"] = agg["bytes_sum"].ewm(span=l, adjust=False).mean()
    agg["bytes_sum_macd"] = agg["bytes_sum_ema_short"] - agg["bytes_sum_ema_long"]
    agg["bytes_sum_macd_signal"] = agg["bytes_sum_macd"].ewm(span=sig, adjust=False).mean()

    return agg


def build_tft_frame_from_raw(
    path_or_df: Union[str, pd.DataFrame],
    *,
    training: bool,
    tz=CST,
    freq: str = "1s",
    ema_macd: EMAMACDConfig = EMAMACDConfig(),
    series_id: int = 0,
    base_datetime: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Convenience wrapper:
      raw events -> aggregated per-second -> adds time_idx + series_id.

    time_idx definition:
      seconds since base_datetime (or min datetime if base_datetime is None)
    """
    agg = aggregate_raw_to_seconds(
        path_or_df,
        training=training,
        tz=tz,
        freq=freq,
        ema_macd=ema_macd,
    )

    if agg.empty:
        # still return expected columns (helps downstream)
        cols = META_COLS + COVARIATE_COLS + ([TARGET_COL] if training else [])
        out = pd.DataFrame(columns=cols + ["time_idx", "series_id"])
        return out

    if base_datetime is None:
        base_datetime = agg["datetime"].min()

    # ensure tz-aware timestamps match
    if getattr(base_datetime, "tzinfo", None) is None:
        # assume it's in the same timezone as agg
        base_datetime = base_datetime.tz_localize(agg["datetime"].dt.tz)

    delta = agg["datetime"] - base_datetime
    agg["time_idx"] = delta.dt.total_seconds().astype(int)
    agg["series_id"] = int(series_id)

    return agg
