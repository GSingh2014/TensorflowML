from base.base_model import BaseModel
from data_loader.data_generator import DataGenerator
import json
import numpy as np
import matplotlib.pyplot as plt
import sklearn
from  sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler


data = DataGenerator()
#randomly initialize weights
w_layer1 = np.random.rand(4, 4)

#sigmod_value_list = []
#
# fig, ax = plt.subplots(num=None, figsize=(14, 6), dpi=80, facecolor='w', edgecolor='k')


def scaleData(data, x_min, x_max):
    #normalize features
    # scaler = MinMaxScaler(feature_range=(0, 1))
    # datacopy = data.copy()
    # datacopy = datacopy.view((float, len(datacopy.dtype.names)))
    # return scaler.fit_transform(datacopy)
    # scaler = StandardScaler()
    # scaler.fit(data)
    # return scaler.fit_transform(data)
    nom = (data-data.min(axis=0))*(x_max-x_min)
    denom = data.max(axis=0) - data.min(axis=0)
    #denom[denom == 0.0] = 1.0
    denom = denom + (denom is 0)
    return x_min + nom/denom


while True:
    # Split the data to get the features
    json_data = json.loads(data.run().split(';')[0])
    print(json_data)
    data.write_to_csv(json_data, '..\\data_loader', 'data_dump.csv')
    # newjson = {k : json_data[k] for k in json_data if k in ['vehicle_speed', 'engine_speed', 'tire_pressure']}
    # featuresCol = ['vehicle_speed', 'engine_speed', 'tire_pressure']
    # basemodel = BaseModel(json_data, featuresCol, w_layer1)
    # sigmoid_value = basemodel.predict(1, 0, 0, -1)
    # newjson["sigmoid_value"] = sigmoid_value
    # #print(newjson)
    # formats = [type(v) for v in newjson.values()]
    # dtype = dict(names=list(newjson.keys()), formats=formats)
    #
    # data_array = np.array(list(newjson.values()), dtype=np.float_)
    #
    # print(repr(data_array))
    #
    # data_array_normalized = scaleData(data_array, -1, 1)
    #
    # print(data_array_normalized)
    #
    # plt.plot(data_array_normalized)
    # plt.pause(0.05)
    #
    # data_array_normalized_reshaped = data_array_normalized

    #reshape to (10, 10, 4) i.e. we have 10 batches of length 10
    #data_array_normalized_reshaped.shape = (samples // timesteps, timesteps, dim)

    #print(data_array_normalized_reshaped)


    #sigmod_value_list.append(sigmoid_value)
    #print(sigmod_value_list)
    # size = len(json_data) - 7
    # print(size)
    # print(json_data['vehicle_speed'], json_data['engine_speed'], json_data['tire_pressure'])
    # ax.scatter(json_data['vehicle_speed'], json_data['vehicle_speed'],color='blue')
    # ax1 = ax.twinx()
    # ax1.scatter(json_data['engine_speed'], json_data['engine_speed'], color='red')
    # ax2 = ax.twinx()
    # ax2.scatter(json_data['tire_pressure'], json_data['tire_pressure'], color='green')
    #plt.scatter(len(sigmod_value_list), sigmoid_value)
    #plt.xlabel('x')
    #plt.ylabel('f(x)')
    #plt.draw()
    # plt.pause(0.05)

plt.show()






