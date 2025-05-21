
import joblib
import pandas
import random

class Model:

    """
    Loads a model via joblib
    """

    def log(self, m):
        print("UseJobLib Model: " + str(m))

    def __init__(self, settings):
        # Load your RF model
        self.log("loading: '%s'" % settings["saved_state"])
        self.model  = joblib.load(settings["saved_state"])

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
            # self.log("new min: " + tokens[6])
        if value > self.max:
            self.max = value
            # self.log("new max: " + tokens[6])
        return True

    def predict(self, data):
        """
        Fill in DURATION for given workload
        return SUCCESS, VALUE
        """

        df_feat = preprocess_workload(raw if isinstance(raw, str) else raw)

        X = df_feat.drop(columns=['TIMESTAMP_last'], errors='ignore')

        # 3) Directly call the model
        preds = self.model.predict(X)

        ts = df_feat.get('TIMESTAMP_last')
        return True, list(zip(ts.astype(str).tolist(), preds.tolist()))
