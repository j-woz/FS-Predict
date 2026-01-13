
import joblib
import pandas
import random
from preprocessing import preprocess_workload
class Model:

    """
    Loads a model via joblib
    """

    def log(self, m):
        print("UseJobLib Model: " + str(m))

    def __init__(self, settings):
        # Load model
        self.log("loading: '%s'" % settings["saved_state"])
        self.model  = joblib.load(settings["saved_state"])
        self.scaler = None
        if "scaler_path" in settings:
            self.log("loading scaler: '%s'" % settings["scaler_path"])
            self.scaler = joblib.load(settings["scaler_path"])
    def insert(self, data):
        self.log("insert: " + str(data))
        if len(data) == 0: return True
        tokens = data.split(",")
        if len(tokens) != 7:
            self.log("insert: bad data: '%s'" % str(tokens))
            return False
        value = float(tokens[6])
        if value < self.min:
            self.min = value
        if value > self.max:
            self.max = value
        return True

    def predict(self, raw):
        df_feat = preprocess_workload(raw if isinstance(raw, str) else raw, training=False)
        X = df_feat.drop(columns=['TIMESTAMP_last'], errors='ignore')

         # NEW: scale features if scaler is available
        if self.scaler is not None:
            from pandas import DataFrame
            X_scaled = self.scaler.transform(X)
            X = DataFrame(X_scaled, columns=X.columns)

        preds = self.model.predict(X)
        ts = df_feat.get('TIMESTAMP_last')
        return True, list(zip(ts.astype(str).tolist(), preds.tolist()))
