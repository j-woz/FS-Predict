
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
        # self.log("initializing")
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
        b = self.model.insert(data)
        return b

    def predict(self, data):
        """ Fill in DURATION for given workload """
        b, value = self.model.predict(data)
        return (b, value)
