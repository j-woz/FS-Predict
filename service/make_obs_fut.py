import os
import argparse
import pandas as pd

from config import program_settings


RAW_COLUMNS = ["TIMESTAMP", "DY", "HR", "MN", "OP", "BYTES", "DURATION"]
DEFAULT_CHUNK_SIZE = 200_000


def _load_full_raw(in_path):
    df_raw = pd.read_csv(in_path, header=None)
    if df_raw.shape[1] != 7:
        raise ValueError(f"Expected 7 columns in {in_path}, got {df_raw.shape[1]}")
    df_raw.columns = RAW_COLUMNS
    return df_raw


def _extract_first_window_streaming(in_path, E, H, chunksize=DEFAULT_CHUNK_SIZE):
    target_count = E + H
    active_seconds = []
    collected_chunks = []
    previous_last_ts = None
    last_obs_ts = None
    future_secs = None
    cutoff_ts = None

    chunk_iter = pd.read_csv(
        in_path,
        header=None,
        names=RAW_COLUMNS,
        chunksize=chunksize,
    )

    for chunk in chunk_iter:
        if chunk.shape[1] != 7:
            raise ValueError(f"Expected 7 columns in {in_path}, got {chunk.shape[1]}")

        chunk_ts = chunk["TIMESTAMP"].astype(int)

        if previous_last_ts is not None and (chunk_ts < previous_last_ts).any():
            return None, None, None, False

        previous_last_ts = int(chunk_ts.iloc[-1])

        unique_secs = chunk_ts.drop_duplicates().tolist()
        if active_seconds and unique_secs and unique_secs[0] == active_seconds[-1]:
            unique_secs = unique_secs[1:]
        active_seconds.extend(unique_secs)

        if cutoff_ts is None and len(active_seconds) >= target_count:
            last_obs_ts = int(active_seconds[E - 1])
            future_secs = active_seconds[E:target_count]
            cutoff_ts = int(active_seconds[target_count - 1])

        if cutoff_ts is None:
            collected_chunks.append(chunk)
            continue

        collected_chunks.append(chunk.loc[chunk_ts <= cutoff_ts].copy())

        if (chunk_ts > cutoff_ts).any():
            break

    if cutoff_ts is None or future_secs is None or last_obs_ts is None:
        return None, None, None, True

    df_raw = pd.concat(collected_chunks, ignore_index=True)
    return df_raw, last_obs_ts, future_secs, True


def _extract_first_window_full_scan(in_path, E, H):
    df_raw = _load_full_raw(in_path)
    sec_list = sorted(df_raw["TIMESTAMP"].astype(int).unique().tolist())

    if len(sec_list) < (E + H):
        raise RuntimeError(
            f"Not enough aggregated seconds in {in_path}. "
            f"Need at least E+H={E+H}, but got {len(sec_list)}."
        )

    last_obs_ts = sec_list[E - 1]
    future_secs = sec_list[E : E + H]
    return df_raw, last_obs_ts, future_secs


def main(
    in_path="data/test.csv",
    out_observed="data/observed.csv",
    out_future="data/future_cov.csv",
    E=50,
    H=20,
):
    # 1) Load only enough raw rows to cover the first valid E+H active seconds.
    df_raw, last_obs_ts, future_secs, in_order = _extract_first_window_streaming(
        in_path, E, H
    )

    if not in_order:
        print("Input timestamps are not sorted; falling back to full-file scan for correctness.")
        df_raw, last_obs_ts, future_secs = _extract_first_window_full_scan(in_path, E, H)

    if df_raw is None or last_obs_ts is None or future_secs is None:
        raise RuntimeError(
            f"Not enough aggregated seconds in {in_path}. "
            f"Need at least E+H={E+H}."
        )

    # 2) Choose split point by aggregated seconds
    future_set = set(future_secs)

    # 3) Build observed raw (<= last_obs_ts)
    df_obs = df_raw[df_raw["TIMESTAMP"].astype(int) <= int(last_obs_ts)].copy()

    # 4) Build future raw: rows whose TIMESTAMP is in the future seconds set
    # then drop DURATION for covariate-only stream
    df_fut = df_raw[df_raw["TIMESTAMP"].astype(int).isin(future_set)].copy()
    df_fut = df_fut.drop(columns=["DURATION"])

    # Add `duration_sum` column to future covariates with NaN values
    df_fut["duration_sum"] = float("nan")

    # 5) Save headerless CSVs (client/server expects header=None format)
    os.makedirs(os.path.dirname(out_observed), exist_ok=True)

    df_obs.to_csv(out_observed, index=False, header=False)
    df_fut.to_csv(out_future, index=False, header=False)

    print("DONE")
    print(f"Input raw:        {in_path}")
    print(f"Observed output:  {out_observed}  (rows={len(df_obs)}, last_obs_ts={last_obs_ts})")
    print(f"Future output:    {out_future}    (rows={len(df_fut)}, future_secs=[{future_secs[0]}..{future_secs[-1]}])")
    print(f"Aggregated secs:  observed={E}, future={H}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Split one raw workload CSV into observed and future TFT input files."
    )
    parser.add_argument(
        "--settings",
        help="Path to YAML settings file (defaults to ./settings.yaml if present)",
    )
    parser.add_argument(
        "-i",
        "--input",
        help="Path to the raw 7-column workload CSV.",
    )
    parser.add_argument(
        "--observed-output",
        help="Path to write the observed-history CSV.",
    )
    parser.add_argument(
        "--future-output",
        help="Path to write the future-covariates CSV.",
    )
    parser.add_argument(
        "-E",
        "--encoder-len",
        type=int,
        help="Number of aggregated active seconds to keep for the observed window.",
    )
    parser.add_argument(
        "-H",
        "--horizon",
        type=int,
        help="Number of aggregated active seconds to keep for the future window.",
    )
    args = parser.parse_args()

    try:
        config = program_settings(args.settings, "make_obs_fut", required=bool(args.settings))
    except (FileNotFoundError, ValueError) as e:
        parser.error(str(e))

    if args.input is None:
        args.input = config.get("input", "data/test.csv")
    if args.observed_output is None:
        args.observed_output = config.get("observed_output", "data/observed.csv")
    if args.future_output is None:
        args.future_output = config.get("future_output", "data/future_cov.csv")
    if args.encoder_len is None:
        args.encoder_len = int(config.get("encoder_len", 50))
    if args.horizon is None:
        args.horizon = int(config.get("horizon", 20))

    return args


if __name__ == "__main__":
    args = parse_args()
    main(
        in_path=args.input,
        out_observed=args.observed_output,
        out_future=args.future_output,
        E=args.encoder_len,
        H=args.horizon,
    )
