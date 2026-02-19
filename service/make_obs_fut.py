import os
import pandas as pd

from preprocessing import aggregate_raw_to_seconds


def main(
    in_path="data/test.csv",
    out_observed="data/observed.csv",
    out_future="data/future_cov.csv",
    E=50,
    H=20,
):
    # 1) Load raw (headerless) 7-col file
    df_raw = pd.read_csv(in_path, header=None)
    if df_raw.shape[1] != 7:
        raise ValueError(f"Expected 7 columns in {in_path}, got {df_raw.shape[1]}")

    df_raw.columns = ["TIMESTAMP", "DY", "HR", "MN", "OP", "BYTES", "DURATION"]

    # 2) Aggregate to per-second to find valid seconds with events
    agg = aggregate_raw_to_seconds(df_raw, training=True)
    if agg.empty:
        raise RuntimeError("Aggregation produced empty dataframe. Check input file.")

    # These are the seconds that have at least one event (because empty seconds are dropped)
    sec_list = agg["TIMESTAMP_last"].astype(int).unique().tolist()
    sec_list = sorted(sec_list)

    if len(sec_list) < (E + H):
        raise RuntimeError(
            f"Not enough aggregated seconds in {in_path}. "
            f"Need at least E+H={E+H}, but got {len(sec_list)}."
        )

    # 3) Choose split point by aggregated seconds
    # last observed second = E-th second in the aggregated timeline
    last_obs_ts = sec_list[E - 1]

    # future seconds = next H aggregated seconds after last_obs_ts
    future_secs = sec_list[E : E + H]
    future_set = set(future_secs)

    # 4) Build observed raw (<= last_obs_ts)
    df_obs = df_raw[df_raw["TIMESTAMP"].astype(int) <= int(last_obs_ts)].copy()

    # 5) Build future raw: rows whose TIMESTAMP is in the future seconds set
    # then drop DURATION for covariate-only stream
    df_fut = df_raw[df_raw["TIMESTAMP"].astype(int).isin(future_set)].copy()
    df_fut = df_fut.drop(columns=["DURATION"])

    # Add `duration_sum` column to future covariates with NaN values
    df_fut["duration_sum"] = float("nan")

    # 6) Save headerless CSVs (client/server expects header=None format)
    os.makedirs(os.path.dirname(out_observed), exist_ok=True)

    df_obs.to_csv(out_observed, index=False, header=False)
    df_fut.to_csv(out_future, index=False, header=False)

    print("DONE")
    print(f"Input raw:        {in_path}")
    print(f"Observed output:  {out_observed}  (rows={len(df_obs)}, last_obs_ts={last_obs_ts})")
    print(f"Future output:    {out_future}    (rows={len(df_fut)}, future_secs=[{future_secs[0]}..{future_secs[-1]}])")
    print(f"Aggregated secs:  observed={E}, future={H}")


if __name__ == "__main__":
    main()
