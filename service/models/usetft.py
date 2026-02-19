import os
import pickle
import numpy as np
import pandas as pd
import torch

from lightning.pytorch import Trainer
from pytorch_forecasting import TimeSeriesDataSet, TemporalFusionTransformer


class Model:
    def log(self, m):
        print("UseTFT Model: " + str(m))

    def __init__(self, settings):
        self.ckpt_path = settings.get("ckpt_path")
        self.dataset_path = settings.get("dataset_path")

        if not self.ckpt_path:
            raise ValueError("usetft requires -k ckpt_path=... (tft .ckpt)")
        if not self.dataset_path:
            raise ValueError("usetft requires -k dataset_path=... (pickled TimeSeriesDataSet)")

        self.device = settings.get("device", "cuda" if torch.cuda.is_available() else "cpu")
        self.batch_size = int(settings.get("batch_size", 64))
        self.num_workers = int(settings.get("num_workers", 0))
        self.quantile = float(settings.get("quantile", 0.5))
        self.horizon = int(settings.get("horizon", 20))

        # IMPORTANT: stop Lightning from thinking it should do distributed
        # (these often exist on clusters / SLURM shells)
        os.environ.pop("WORLD_SIZE", None)
        os.environ.pop("RANK", None)
        os.environ.pop("LOCAL_RANK", None)
        os.environ.pop("NODE_RANK", None)

        # covariates must match what training dataset used
        self.known_reals = [
            "bytes_op0", "bytes_op1", "bytes_sum", "io_count",
            "read_ops_count", "write_ops_count",
            "bytes_sum_ema_short", "bytes_sum_ema_long",
            "bytes_sum_macd", "bytes_sum_macd_signal",
        ]

        self.log(f"Loading training dataset from: {self.dataset_path}")
        with open(self.dataset_path, "rb") as f:
            self.training_dataset = pickle.load(f)

        self.log(f"Loading TFT checkpoint from: {self.ckpt_path}")
        self.model = TemporalFusionTransformer.load_from_checkpoint(self.ckpt_path)
        self.model.to(self.device)
        self.model.eval()

        self.max_encoder_length = getattr(self.training_dataset, "max_encoder_length", None)
        self.max_prediction_length = getattr(self.training_dataset, "max_prediction_length", None)

        # ✅ Create ONE trainer (single device, no spawn, no logger)
        accel = "gpu" if (self.device.startswith("cuda") and torch.cuda.is_available()) else "cpu"
        self.trainer = Trainer(
            accelerator=accel,
            devices=1,
            logger=False,
            enable_checkpointing=False,
            enable_progress_bar=False,
            enable_model_summary=False,
            strategy="auto",   # with devices=1 this stays single-process
        )

        self.log(
            f"device={self.device}, batch_size={self.batch_size}, quantile={self.quantile}, "
            f"dataset(E={self.max_encoder_length}, H={self.max_prediction_length}), horizon_arg={self.horizon}"
        )

    def insert(self, data):
        self.log("insert(): not supported for TFT deployment (no-op).")
        return True

    @torch.no_grad()
    def predict(self, raw):
        """
        raw: per-second DataFrame created by server.py _build_inference_frame()

        Must include:
          - time_idx (int), series_id (int)
          - TIMESTAMP_last (int seconds)
          - covariates in self.known_reals
          - duration_sum (float; NaN for future rows)
        """
        try:
            df = raw.copy()
            if not isinstance(df, pd.DataFrame):
                return False, "predict() expected a pandas DataFrame"

            # Required columns (must match training dataset)
            required = ["time_idx", "series_id", "TIMESTAMP_last", "duration_sum"] + self.known_reals
            missing = [c for c in required if c not in df.columns]
            if missing:
                return False, f"Missing columns for TFT predict: {missing}"

            # dtypes
            df["time_idx"] = df["time_idx"].astype(int)
            df["series_id"] = df["series_id"].astype(int)
            df["TIMESTAMP_last"] = df["TIMESTAMP_last"].astype(int)

            # sort
            df = df.sort_values("time_idx").reset_index(drop=True)

            # IMPORTANT: save future mask BEFORE filling NaNs
            fut_mask = df["duration_sum"].isna()

            if not fut_mask.any():
                return False, "No future rows (duration_sum NaN) found in input frame"

            # expected horizon
            expected_h = int(self.max_prediction_length) if self.max_prediction_length is not None else self.horizon

            # future timestamps from mask
            future_ts = df.loc[fut_mask, "TIMESTAMP_last"].astype(int).tolist()
            if len(future_ts) > expected_h:
                future_ts = future_ts[:expected_h]

            # PyTorch-Forecasting cannot encode NaN targets -> fill them
            # Option 1: fill with 0.0
            df.loc[fut_mask, "duration_sum"] = 0.0

            # Option 2 (alternative): fill with last observed duration_sum
            # last_obs_val = df.loc[~fut_mask, "duration_sum"].iloc[-1]
            # df.loc[fut_mask, "duration_sum"] = float(last_obs_val)

            # Final check: no NaNs/inf allowed
            if df["duration_sum"].isna().any() or np.isinf(df["duration_sum"].values).any():
                return False, "duration_sum still contains NaN/inf after filling"

            # Build pred dataset using training dataset settings (scalers/normalizers/etc.)
            pred_ds = TimeSeriesDataSet.from_dataset(
                self.training_dataset,
                df,
                predict=True,
                stop_randomization=True,
                allow_missing_timesteps=True,
            )

            pred_loader = pred_ds.to_dataloader(
                train=False,
                batch_size=self.batch_size,
                num_workers=self.num_workers,
            )

             # Using mode="prediction" to avoid dict object error
            pred = self.model.predict(
                pred_loader,
                mode="prediction",  # Change to mode="prediction" for point forecast
                return_x=False,
            )

            # pred can be torch tensor or numpy
            if hasattr(pred, "detach"):
                pred = pred.detach().cpu().numpy()
            else:
                pred = np.asarray(pred)

            # pred shape often: [n_samples, pred_len]
            if pred.ndim == 2:
                seq = pred[0]
            elif pred.ndim == 1:
                seq = pred
            else:
                return False, f"Unexpected prediction output shape: {pred.shape}"

            # Align with future timestamps
            future_ts = df["TIMESTAMP_last"].iloc[-self.horizon:].tolist()
            L = min(len(seq), len(future_ts))
            out = [(str(future_ts[i]), float(seq[i])) for i in range(L)]
            print(out)
            return True, out

        except Exception as e:
            return False, f"TFT predict failed: {e}"