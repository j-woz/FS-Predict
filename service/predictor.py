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
        keyvalue: The list of command-line "key=value" strings
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

        settings = self.scan_settings(keyvalue)
        
        # Initialize model (use the 'Model' class from the specified module)
        self.model = module.Model(settings)

    def scan_settings(self, keyvalue):
        settings = {}
        if keyvalue is None: return settings
        for kv in keyvalue:
            tokens = kv.split("=")
            if len(tokens) != 2:
                raise(Exception("bad keyvalue pair: '%s'" % kv))
            settings[tokens[0]] = tokens[1]
        return settings
        
    def insert(self, data):
        """ Insert small recent measurements (not used in Option A) """
        b = self.model.insert(data)
        return b

    def predict(self, raw):
        """ Fill in DURATION for given workload (prediction for TFT) """
        # Returns a tuple of success flag and predicted values
        b, value = self.model.predict(raw)
        return (b, value)

    def save(self, filename):
        """ Save model checkpoint """
        self.model.save(filename)

    def load(self, filename):
        """ Load model checkpoint """
        self.model.load(filename)
