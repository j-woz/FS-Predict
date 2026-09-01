import numpy as np

"""
PREDICTOR
The plain math-level prediction interface

Base class for the model implementations in models/*.py.
Use Predictor.create() to load one by name.
"""

class Predictor:

    def log(self, m):
        print("predictor: " + m)

    @classmethod
    def create(cls, model_name, keyvalue):
        """
        Import models/<model_name>.py and instantiate its Model.
        Returns None if the import fails.
        keyvalue: The list of command-line "key=value" strings
        """
        print("predictor: initializing: model: '%s'" % model_name)
        import importlib
        try:
            module = importlib.import_module(model_name)
        except ImportError as e:
            print("predictor: init failed: " + str(e))
            return None

        settings = cls.scan_settings(keyvalue)

        # Initialize model (use the 'Model' class from the specified module)
        return module.Model(settings)

    @staticmethod
    def scan_settings(keyvalue):
        settings = {}
        if keyvalue is None: return settings
        for kv in keyvalue:
            tokens = kv.split("=")
            if len(tokens) != 2:
                raise(Exception("bad keyvalue pair: '%s'" % kv))
            settings[tokens[0]] = tokens[1]
        return settings

    def observe(self, data):
        """ Feed recent observed measurements to the model """
        raise NotImplementedError("%s does not implement observe()"
                                  % type(self).__name__)

    def predict(self, raw):
        """ Fill in DURATION for given workload (prediction for TFT) """
        # Returns a tuple of success flag and predicted values
        raise NotImplementedError("%s does not implement predict()"
                                  % type(self).__name__)

    def save(self, filename):
        """ Save model checkpoint """
        raise NotImplementedError("%s does not implement save()"
                                  % type(self).__name__)

    def load(self, filename):
        """ Load model checkpoint """
        raise NotImplementedError("%s does not implement load()"
                                  % type(self).__name__)
