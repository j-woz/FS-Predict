
"""
PREDICTOR
The plain math-level prediction interface
"""

class Predictor:

    def log(self, m):
        print("predictor: " + m)

    def __init__(self, name):
        """
        If module import fails, sets self.model==None
        """
        self.log("initializing")
        # Select model implementation
        import importlib
        try:
            module = importlib.import_module(name)
        except ImportError as e:
            self.log("init failed: " + str(e))
            self.model = None
            return
        self.model = module.Model()

    def insert(self, data):
        """ Insert small recent measurements """
        self.log("insert: " + data)
        # Hand off to real math model
        # self.model.insert(data)

    def predict(self, data):
        """ Fill in DURATIONs for given workload """
        self.log("predict: " + data)
        return "prediction"
