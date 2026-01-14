
"""
Class name sushil_model_1.Model
Feel free to rename!
"""

import random

from retrain import retrain_mlp_model

class Model:

    """
    """

    def log(self, m):
        print("RNG Model: " + str(m))

    def __init__(self,settings = None):
        # self.log("initializing...")
        # Initial bounds:
        self.min = 1000
        self.max = 0

    def insert(self, data):
        """ data: a DataFrame """
        # Retrain...
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
        data: a DataFrame
        return SUCCESS, VALUE
        """
        self.log("predict: " + str(data))
        if len(data) == 0: return (True, None)
        tokens = data.split(",")
        if len(tokens) != 6:
            self.log("predict: bad data: '%s'" % str(tokens))
            return False, None
        value = random.uniform(self.min, self.max)
        self.log("predict: \t -> %0.6f" % value)
        return (True, value)
