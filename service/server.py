"""
XFER SERVER (OBSERVE + PREDICT streaming for TFT)

Commands:
  - observe: stream raw rows with DURATION (7 cols) -> update history buffer (encoder)
  - predict: stream raw rows without DURATION (6 cols) for future horizon -> update future buffer (decoder)
  - quit: stop server

Once:
  history buffer has E seconds (default 50)
  AND future buffer has H seconds (default 20, contiguous next seconds)

server builds a single inference instance and calls predictor.predict(combined_df)
and streams back predictions as:
  "<timestamp>,<pred>\n"
until EOF
"""

import os, socket, sys, time
import numpy as np
from utils import send, recv_line
import pandas as pd
import io
os.environ.setdefault("CUDA_VISIBLE_DEVICES", "0")
from predictor import Predictor
from preprocessing import aggregate_raw_to_seconds

cancelled = False  #  False until we are shutting down,
                   #        possibly due to a signal
sock = None
sockfile = None
predictor = None

# models dir
sys.path.append(os.path.join(os.path.dirname(__file__), "models"))
settings = {}  # parsed -k key=value pairs

# === STREAMING BUFFERS ===
HISTORY_E = 50   # encoder length
FUTURE_H  = 20   # prediction horizon

history_buf = pd.DataFrame()   # per-second rows with duration_sum
future_buf  = pd.DataFrame()   # per-second rows without duration_sum


def main():
    global sock, predictor, settings, HISTORY_E, FUTURE_H
    args = parse_args()
    settings = parse_keyvals(args.keyvalue)

    # allow overriding E/H from command line
    if "encoder_len" in settings:
        HISTORY_E = int(settings["encoder_len"])
    if "horizon" in settings:
        FUTURE_H = int(settings["horizon"])

    predictor = Predictor(args.model, args.keyvalue)
    if predictor.model is None:
        exit(1)

    sock = make_socket(args)
    if sock is None:
        abort("could not make socket!")
        exit(1)

    code = run_server(args)
    shutdown(args, code)


def msg(m):
    print("xfer-server: " + str(m))
    sys.stdout.flush()


def warn(m):
    msg("warning: " + str(m))


def abort(m):
    msg("abort: " + str(m))


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Run server")
    parser.add_argument(
        "-k", "--keyvalue",
        action="append",
        help="A key/value setting for the underlying model"
    )
    parser.add_argument("-m", "--model", required=True, help="Model module to import (e.g. usetft)")
    parser.add_argument("-s", "--socket", help="Local socket path")
    args = parser.parse_args()
    print(str(args))
    return args


def parse_keyvals(keyvals):
    result = {}
    if keyvals:
        for kv in keyvals:
            if "=" in kv:
                k, v = kv.split("=", 1)
                result[k] = v
    return result


def make_socket(args):
    import random
    global sockfile, sock
    tmp = get_tmp()
    if tmp is None:
        return None
    index = random.randint(1, 1000)
    if args.socket is None:
        sockfile = tmp + "/xfer-sock." + str(index) + ".s"
    else:
        sockfile = args.socket

    if not make_sock_dir(sockfile):
        return None
    if not reset_sock(sockfile):
        return None

    msg("opening socket: " + sockfile)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(sockfile)
        msg(f"Socket bound successfully at {sockfile}") 
    except Exception as e:
        abort(f"could not open socket at '{sockfile}': {e}")
        sock = None
    return sock


def get_tmp():
    """Make a temp directory - see README"""
    tmp = None
    if os.getenv("XFER_TMP") is not None:
        tmp = os.getenv("XFER_TMP")
    elif os.getenv("TMPDIR") is not None:
        tmp = os.getenv("TMPDIR") + "/xfer-tmp"
    elif os.getenv("TMP") is not None:
        tmp = os.getenv("TMP") + "/xfer-tmp"
    else:
        tmp = "/tmp/" + os.getenv("USER") + "/xfer-tmp"
    try:
        os.makedirs(tmp, exist_ok=True)
    except Exception as e:
        abort(str(e))
        tmp = None
    return tmp


def make_sock_dir(sockfile):
    directory = os.path.dirname(sockfile)
    try:
        os.makedirs(directory, exist_ok=True)
    except Exception as e:
        abort("failed making socket directory: " + str(e))
        return False
    return True


def reset_sock(sockfile):
    if os.path.exists(sockfile):
        msg("resetting: " + sockfile)
        try:
            os.remove(sockfile)
        except Exception as e:
            abort(str(e))
            return False
    return True


def run_server(args):
    global sock
    if sock is None:
        shutdown(args, 1)

    handlers = setup_handlers()
    sock.settimeout(0.1)

    while not cancelled:
        sock.listen(1)
        msg("accepting ...")

        connected = False
        while not cancelled and not connected:
            try:
                conn, addr = sock.accept()
                connected = True
            except socket.timeout:
                pass
            except Exception as e:
                abort("accept failed: " + str(e))
                return 1

        msg("client is connected.")
            
        if cancelled:
            msg("cancelled accept loop...")
            break

        try:
            datagram = conn.recv(1024)
        except Exception as e:
            warn("connection dropped: " + str(e))
            datagram = None

        if datagram is not None:
            tokens = datagram.decode().strip().split()
            handle(handlers, conn, tokens)

        time.sleep(0.1)

    return 0


def setup_handlers():
    """Setup both signal handlers and client service method handlers"""
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

    return {
        "observe": do_observe,
        "predict": do_predict,
        "quit": do_quit,
    }


def signal_handler(unused_sig, unused_frame):
    global cancelled, sock
    print("\nxfer-server: cancelled!\n")
    if cancelled:
        print("\nxfer-server: closing socket!\n")
        sock.close()
    cancelled = True


def handle(handlers, conn, tokens):
    if len(tokens) != 1:
        send(conn, "ERROR\n")
        return
    method = tokens[0]
    if method not in handlers:
        warn(f"unknown method: '{method}'")
        send(conn, f"ERROR: unknown method {method}\n")
        return
    handlers[method](conn, tokens)


def _recv_lines_until_eof(conn):
    """Receive streamed lines until 'EOF'."""
    raw_lines = []
    L = []
    while True:
        line = recv_line(conn, L)
        if line is None:
            return None
        if line.strip() == "EOF":
            break
        raw_lines.append(line)
    return raw_lines


def _update_history(agg_obs: pd.DataFrame):
    global history_buf, HISTORY_E

    if agg_obs is None or agg_obs.empty:
        return

    agg_obs = agg_obs.copy()
    agg_obs["TIMESTAMP_last"] = agg_obs["TIMESTAMP_last"].astype(int)

    if history_buf.empty:
        history_buf = agg_obs
    else:
        history_buf = pd.concat([history_buf, agg_obs], ignore_index=True)

    history_buf = history_buf.drop_duplicates(subset=["TIMESTAMP_last"], keep="last")
    history_buf = history_buf.sort_values("TIMESTAMP_last").reset_index(drop=True)

    # Make sure we have a contiguous last E seconds (fill gaps with zeros)
    last_ts = int(history_buf["TIMESTAMP_last"].iloc[-1])
    lo = last_ts - HISTORY_E + 1
    hi = last_ts

    history_buf = _fill_missing_seconds(history_buf, lo, hi, training=True)
    


KNOWN_REALS = [
    "bytes_op0", "bytes_op1", "bytes_sum", "io_count",
    "read_ops_count", "write_ops_count",
    "bytes_sum_ema_short", "bytes_sum_ema_long",
    "bytes_sum_macd", "bytes_sum_macd_signal",
]

def _fill_missing_seconds(df_sec: pd.DataFrame, lo: int, hi: int, training: bool) -> pd.DataFrame:
    """
    Ensure per-second dataframe has *all* seconds in [lo..hi].
    Missing seconds are filled with zeros (covariates) and:
      - duration_sum = 0 for training/history (or keep if present)
      - duration_sum = NaN for future (so TFT predicts)
    """
    if lo > hi:
        return pd.DataFrame()

    base = pd.DataFrame({"TIMESTAMP_last": list(range(lo, hi + 1))})
    if df_sec is None or df_sec.empty:
        out = base
    else:
        tmp = df_sec.copy()
        tmp["TIMESTAMP_last"] = tmp["TIMESTAMP_last"].astype(int)
        out = base.merge(tmp, on="TIMESTAMP_last", how="left")

    # Fill covariates for missing seconds
    for c in KNOWN_REALS:
        if c not in out.columns:
            out[c] = 0.0
        out[c] = out[c].fillna(0.0)

    # duration_sum handling
    if "duration_sum" not in out.columns:
        out["duration_sum"] = np.nan if not training else 0.0
    else:
        if training:
            out["duration_sum"] = out["duration_sum"].fillna(0.0)
        else:
            # future: must be NaN (unknown target)
            out["duration_sum"] = np.nan

    # required ids
    out["time_idx"] = out["TIMESTAMP_last"].astype(int)
    out["series_id"] = 0
    return out


def _update_future(agg_fut: pd.DataFrame):
    """
    agg_fut: per-second aggregated WITHOUT duration_sum
    Maintains global future_buf with next FUTURE_H seconds after last observed timestamp.
    """
    global future_buf, FUTURE_H, history_buf
    if agg_fut.empty:
        return

    if history_buf.empty:
        # can't define "future" without knowing last observed second
        return

    last_obs_ts = int(history_buf["TIMESTAMP_last"].iloc[-1])
    lo = last_obs_ts + 1
    hi = last_obs_ts + FUTURE_H

    # Keep only rows in (last_obs_ts, last_obs_ts+H]
    agg_fut = agg_fut.copy()
    agg_fut["TIMESTAMP_last"] = agg_fut["TIMESTAMP_last"].astype(int)
    agg_fut = agg_fut[(agg_fut["TIMESTAMP_last"] >= lo) & (agg_fut["TIMESTAMP_last"] <= hi)]

    # Ensure duration_sum exists for future data
    agg_fut["duration_sum"] = np.nan  # Future rows must have NaN duration_sum for prediction

    # Fill missing seconds within the future horizon
    future_buf = _fill_missing_seconds(agg_fut, lo, hi, training=False)

    




def _ready_for_inference() -> bool:
    global history_buf, future_buf, HISTORY_E, FUTURE_H
    return (not history_buf.empty and len(history_buf) == HISTORY_E and
            not future_buf.empty and len(future_buf) == FUTURE_H)



def _force_future_duration_nan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure duration_sum exists and is NaN for ALL rows in df.
    (Future rows must be unknown targets for TFT.)
    """
    df = df.copy()
    df["duration_sum"] = np.nan
    return df


def _build_inference_frame() -> pd.DataFrame:
    """
    Returns combined per-second DataFrame: last E history + next H future covariates
    """
    global history_buf, future_buf, HISTORY_E, FUTURE_H

    # Step 1: Extract last E history rows
    hist = history_buf.iloc[-HISTORY_E:].copy()
    hist["TIMESTAMP_last"] = hist["TIMESTAMP_last"].astype(int)
    hist["time_idx"] = hist["TIMESTAMP_last"].astype(int)
    hist["series_id"] = 0

    # Step 2: Find the last observed timestamp
    last_obs_ts = int(hist["TIMESTAMP_last"].iloc[-1])
    lo = last_obs_ts + 1
    hi = last_obs_ts + FUTURE_H

    # Step 3: Extract the future data within the time range (lo, hi)
    fut = future_buf.copy()
    fut = fut[(fut["TIMESTAMP_last"] >= lo) & (fut["TIMESTAMP_last"] <= hi)].copy()
    fut["TIMESTAMP_last"] = fut["TIMESTAMP_last"].astype(int)
    fut["time_idx"] = fut["TIMESTAMP_last"].astype(int)
    fut["series_id"] = 0

    # Step 4: Add `duration_sum` column to future data with NaN values (since we are predicting it)
    fut["duration_sum"] = float("nan")  # Ensure that future `duration_sum` is NaN

    # Step 5: Combine historical and future data
    combined = pd.concat([hist, fut], ignore_index=True)
    combined = combined.sort_values("TIMESTAMP_last").reset_index(drop=True)

    
    return combined






def do_observe(conn, tokens):
    """
    OBSERVE: client streams 7-col raw events (incl DURATION).
    Server aggregates to per-second (incl duration_sum) and updates history buffer.
    """
    global cancelled
    msg("do_observe()...")
    send(conn, "OK\n")

    raw_lines = _recv_lines_until_eof(conn)
    if raw_lines is None:
        msg("connection dropped during observe")
        return

    if len(raw_lines) == 0:
        msg("observe: empty payload")
        send(conn, "EOF\n")
        return

    csv_data = "".join(raw_lines)
    df_raw = pd.read_csv(
        io.StringIO(csv_data),
        header=None,
        names=["TIMESTAMP", "DY", "HR", "MN", "OP", "BYTES", "DURATION"],
    )

    agg_obs = aggregate_raw_to_seconds(df_raw, training=True)
    _update_history(agg_obs)

    msg(f"observe: history_buf seconds={len(history_buf)} (need {HISTORY_E})")
    send(conn, "EOF\n")


def do_predict(conn, tokens):
    """
    PREDICT: client streams 6-col raw events (no DURATION) for future horizon.
    Server aggregates covariates, updates future buffer, and if ready runs inference.
    """
    global cancelled, predictor
    msg("do_predict()...")
    send(conn, "OK\n")

    raw_lines = _recv_lines_until_eof(conn)
    if raw_lines is None:
        msg("connection dropped during predict")
        return

    if history_buf.empty or len(history_buf) < HISTORY_E:
        msg("predict: not enough history for encoder")
        send(conn, f"ERROR: need {HISTORY_E} seconds of history, have {len(history_buf)}\n")
        send(conn, "EOF\n")
        return

    if len(raw_lines) == 0:
        msg("predict: empty payload")
        send(conn, "ERROR: empty predict payload\n")
        send(conn, "EOF\n")
        return

    csv_data = "".join(raw_lines)
    df_raw = pd.read_csv(
        io.StringIO(csv_data),
        header=None,
        names=["TIMESTAMP", "DY", "HR", "MN", "OP", "BYTES"],
    )

    agg_fut = aggregate_raw_to_seconds(df_raw, training=False)
    _update_future(agg_fut)

    msg(f"predict: future_buf seconds={len(future_buf)} (need {FUTURE_H})")

    if not _ready_for_inference():
        last_obs_ts = int(history_buf["TIMESTAMP_last"].iloc[-1])
        lo = last_obs_ts + 1
        hi = last_obs_ts + FUTURE_H
        send(conn, f"ERROR: need future covariates for seconds [{lo}..{hi}]\n")
        send(conn, "EOF\n")
        return

    # Build combined inference frame and predict
    combined = _build_inference_frame()
    msg(f"predict: built inference frame rows={len(combined)} (E+H={HISTORY_E+FUTURE_H})")

    success, preds = predictor.predict(combined)
    if not success:
        msg(f"predict: model error: {preds}")
        send(conn, f"ERROR: {preds}\n")
        send(conn, "EOF\n")
        return

    msg(f"Server sending predictions to client... predictions count: {len(preds)}")
    for i, (ts, value) in enumerate(preds):
        msg(f"Sending prediction {i}: {ts}, {value}")
        send(conn, f"{ts},{value}\n")

    send(conn, "EOF\n")
    msg("do_predict(): done.")


def do_quit(conn, tokens):
    global cancelled
    msg("do_quit()...")
    send(conn, "OK\n")
    cancelled = True


def shutdown(args, code):
    global sock, sockfile
    if sock is not None:
        msg("closing socket ...")
        sock.close()
    if args.socket is None and sockfile and os.path.exists(sockfile):
        # We only remove the socket if we created it as a temp file,
        # not if the user provided it.
        msg("removing sock file ...")
        os.remove(sockfile)
    msg("shutdown.")
    exit(code)


if __name__ == "__main__":
    main()
