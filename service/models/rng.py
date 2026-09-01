
"""
Class name rng.Model
"""

import random

import pandas as pd

from predictor import Predictor


class Model(Predictor):

    """
    RNG = Random Number Generator Model
    Returns random predictions in range of observed values
    Serves as an example for future Model implementations
    """

    def log(self, m):
        print("RNG Model: " + str(m))

    def __init__(self, settings=None):
        # self.log("initializing...")
        # Observed bounds, learned from the history rows of each frame:
        self.min = None
        self.max = None

    def _observe_bounds(self, observed):
        """ observed: Series of known duration_sum values """
        values = observed.dropna()
        if len(values) == 0:
            return
        lo = float(values.min())
        hi = float(values.max())
        self.min = lo if self.min is None else min(self.min, lo)
        self.max = hi if self.max is None else max(self.max, hi)

    def observe(self, data):
        """
        data: per-second observed DataFrame from server.py do_observe()

        Learns the range that predictions are later drawn from.
        """
        if not isinstance(data, pd.DataFrame):
            return False
        if len(data) == 0: return True
        if "duration_sum" not in data.columns:
            self.log("observe: no duration_sum column")
            return False
        self._observe_bounds(data["duration_sum"])
        self.log("observe: range is now [%0.6f, %0.6f]" % (self.min, self.max))
        return True

    def predict(self, raw):
        """
        raw: per-second DataFrame created by server.py _build_inference_frame()

        Must include:
          - TIMESTAMP_last (int seconds)
          - duration_sum (float; NaN for future rows)

        return SUCCESS, [(timestamp, value), ...]
        """
        if not isinstance(raw, pd.DataFrame):
            return False, "predict() expected a pandas DataFrame"
        if len(raw) == 0: return (True, [])

        missing = [c for c in ("TIMESTAMP_last", "duration_sum")
                   if c not in raw.columns]
        if missing:
            return False, f"Missing columns for RNG predict: {missing}"

        df = raw.sort_values("TIMESTAMP_last")

        # Future rows are the ones whose target is still unknown
        fut_mask = df["duration_sum"].isna()
        if not fut_mask.any():
            return False, "No future rows (duration_sum NaN) found in input frame"

        # Bounds come from what observe() has been shown so far
        if self.min is None:
            return False, "No observed data yet: run observe before predict"

        out = []
        for ts in df.loc[fut_mask, "TIMESTAMP_last"].astype(int):
            value = random.uniform(self.min, self.max)
            self.log("predict: %d \t -> %0.6f" % (ts, value))
            out.append((int(ts), value))

        return (True, out)
