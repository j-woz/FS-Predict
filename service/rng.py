
class Model:

    def log(self, m):
        print("RNG Model: " + str(m))

    def __init__(self):
        self.log("initializing...")

    def insert(self, data):
        log("insert: " + str(data))

    def predict(self, data):
        log("predict: " + str(data))
        return "prediction"
