import math
import numpy as np


class BaseModel:
    def __init__(self, dict_data_points, features, weights):
        self.data_points = dict_data_points
        self.num_of_features = len(dict_data_points)
        self.features = features
        self.weights = weights

    def sigmoid(self, x):
        return 1.0 / (1.0 + np.exp(-1.0 * x))

    def predict(self, a, b, c, d):
        #return self.data_points.keys()
        #feature_name = list(self.data_points.keys())
        feature_name = self.features
        #print(feature_name)
        return self.sigmoid(a + b * self.data_points[feature_name[0]] +
                            c * self.data_points[feature_name[1]] +
                            d * self.data_points[feature_name[2]])
        #return self.neuron1()

    def neuron1(self):
        x = np.array([1, self.data_points[self.features[0]], self.data_points[self.features[1]],
                      self.data_points[self.features[2]]])
        return self.sigmoid(x.dot(self.weights))
