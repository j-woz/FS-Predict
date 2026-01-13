
from preprocessing import preprocess_workload
import numpy as np

"""
PREDICTOR
The plain math-level prediction interface
"""

class Predictor:

    def log(self, m):
        print("predictor: " + m)

    def __init__(self, model_name, keyvalue):
        """
        If module import fails, sets self.model==None
        settings: The list of command-line key=value pairs
        """
        self.log("initializing: model: '%s'" % model_name)
        # Select model implementation
        import importlib
        try:
            module = importlib.import_module(model_name)
        except ImportError as e:
            self.log("init failed: " + str(e))
            self.model = None
            return
        settings = {}
        for kv in keyvalue:
            tokens = kv.split("=")
            if len(tokens) != 2:
                raise(Exception("bad keyvalue pair: '%s'" % kv))
            settings[tokens[0]] = tokens[1]
        self.model = module.Model(settings)

    def insert(self, data):
        """ Insert small recent measurements """
        b = self.model.insert(data)
        return b

    def predict(self, raw):
        """ Fill in DURATION for given workload """
        b, value = self.model.predict(raw)
        return (b, value)
