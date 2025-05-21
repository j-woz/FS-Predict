import joblib
import pandas as pd
from preprocessing import preprocess_workload
import numpy as np
"""
PREDICTOR
The plain math-level prediction interface
"""

class Predictor:

    def log(self, m):
        print("predictor: " + m)

    def __init__(self, model_path):
        # Load your RF model 
        self.model  = joblib.load(model_path)
       
        """
        If module import fails, sets self.model==None
        """
        # self.log("initializing")
        # Select model implementation
        # import importlib
        # try:
        #     module = importlib.import_module(name)
        # except ImportError as e:
        #     self.log("init failed: " + str(e))
        #     self.model = None
        #     return
        # self.model = module.Model()

    def insert(self, data):
        """ Insert small recent measurements """
        b = self.model.insert(data)
        return b

    def predict(self, raw):
        """ Fill in DURATION for given workload """
        # b, value = self.model.predict(data)
        # return (b, value)
        
        df_feat = preprocess_workload(raw if isinstance(raw, str) else raw)
        
        X = df_feat.drop(columns=['TIMESTAMP_last'], errors='ignore')

        # 3) Directly call the model
        preds = self.model.predict(X)

        ts = df_feat.get('TIMESTAMP_last')
        return True, list(zip(ts.astype(str).tolist(), preds.tolist()))