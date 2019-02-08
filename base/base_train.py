import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import numpy as np
from sklearn.exceptions import DataConversionWarning
import warnings
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.preprocessing import FunctionTransformer
import datetime
import math
import random

from sklearn.decomposition import PCA

import collections
from itertools import count
import matplotlib.pyplot as plt
import seaborn as sn
from matplotlib import style
style.use('fivethirtyeight')

train = pd.read_csv('..\data_loader\data_dump.csv', nrows=1000)
#print(train[:10])
is_anomaly_df = train['is_anomaly']
#print(is_anomaly_df[:10])
train.drop('is_anomaly', axis=1, inplace=True)
#print(train[:10].to_string())

# drop direction from the dataframe
train.drop('direction', axis=1, inplace=True)
#print(train.columns.values)


def timeEncoding(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute
    df['seconds'] = df['timestamp'].dt.second
    df['weekday'] = df['timestamp'].dt.weekday
    df['epoch'] = (df['timestamp'] - pd.to_datetime('1970-01-01')).dt.total_seconds()
    df.drop('timestamp', axis=1, inplace=True)
    return df


# convert string date into features of year, month, day, hour, weekday

train = timeEncoding(train)
#print(train[:10].to_string())


# Normalize the dataframe

warnings.filterwarnings(action='ignore', category=DataConversionWarning)
scaler = StandardScaler()
feature_df = train[['vehicle_speed', 'engine_speed', 'tire_pressure']]
#feature_df = train[['vehicle_speed']]
scaler.fit(feature_df)
norm_feature_np_ndarray = scaler.transform(feature_df)

#print(norm_train_df[:10])

train['norm_features'] = norm_feature_np_ndarray.tolist()

train['normalized_vehicle_speed'] = train['norm_features'].apply(lambda x: x[0])
train['normalized_engine_speed'] = train['norm_features'].apply(lambda x: x[1])
train['normalized_tire_pressure'] = train['norm_features'].apply(lambda x: x[2])

pd.options.display.max_colwidth = 200

train = train.drop(['norm_features'], axis=1)


train.plot(x='minute', y=['normalized_vehicle_speed', 'normalized_engine_speed', 'normalized_tire_pressure'], kind='line')
#plt.show()
# from itertools import islice
#
# def window(seq, n=2):
#     "Returns a sliding window (of width n) over data from the iterable"
#     "   s -> (s0,s1,...s[n-1]), (s1,s2,...,sn), ...                   "
#     it = iter(seq)
#     result = tuple(islice(it, n))
#     if len(result) == n:
#         yield result
#     for elem in it:
#         result = result[1:] + (elem,)
#         yield result
#
#
# def moving_averages(values, size):
#     for selection in window(values, size):
#         yield sum(selection) / size


def moving_average(norm_feature_np_ndarray, window_size):

    """ Computes moving average using discrete linear convolution of 2 1-D sequences
    Args:
    -----
            data (pandas.dataframe): independent variable
            window_size (int): rolling window size

    Returns:
    --------
            ndarray of linear convolution
    """

    window = np.ones(int(window_size))/float(window_size)
    #print(np.ndarray.flatten(df)[:10])
    return np.convolve(np.ndarray.flatten(norm_feature_np_ndarray), window, 'same')


def explain_anomalies(y, window_size, sigma=1.0):
    """ Helps in exploring the anamolies using stationary standard deviation
      Args:
      -----
          y (pandas.Series): independent variable
          window_size (int): rolling window size
          sigma (int): value for standard deviation

      Returns:
      --------
          a dict (dict of 'standard_deviation': int, 'anomalies_dict': (index: value))
          containing information about the points indentified as anomalies

      """
    avg = moving_average(y, window_size).tolist()
    #print(avg[:10])
    #print(np.ndarray.flatten(y)[:10])
    residual = np.ndarray.flatten(y) - avg
    # Calculate the variation in the distribution of the residual
    std = np.std(residual)
    return {'standard_deviation': round(std, 3),
            'anomalies_dict': collections.OrderedDict([(index, y_i) for
                                                       index, y_i, avg_i in zip(count(), y, avg)
                                                       if ((y_i > avg_i + (sigma*std)) | (y_i < avg_i - (sigma*std))).any()])}


def explain_anomalies_rolling_std(y, window_size, sigma=1.0):
    """ Helps in exploring the anamolies using rolling standard deviation
    Args:
    -----
        y (pandas.Series): independent variable
        window_size (int): rolling window size
        sigma (int): value for standard deviation

    Returns:
    --------
        a dict (dict of 'standard_deviation': int, 'anomalies_dict': (index: value))
        containing information about the points indentified as anomalies
    """
    avg = moving_average(y, window_size)
    avg_list = avg.tolist()
    residual = np.ndarray.flatten(y) - avg
    # Calculate the variation in the distribution of the residual
    testing_std_as_df = pd.DataFrame(residual).rolling(window=window_size).std()
    rolling_std = testing_std_as_df.replace(np.nan,
                                            testing_std_as_df.iloc[window_size - 1]).round(3).iloc[:, 0].tolist()
    std = np.std(residual)
    return {'stationary standard_deviation': round(std, 3),
            'anomalies_dict': collections.OrderedDict([(index, y_i)
                                                       for index, y_i, avg_i, rs_i in zip(count(),
                                                                                           y, avg_list, rolling_std)
                                                       if ((y_i > avg_i + (sigma * rs_i)) | (y_i < avg_i - (sigma * rs_i))).any()])}


moving_avg_np_array = moving_average(norm_feature_np_ndarray, 10)

#print(moving_avg_np_array[:10])

train['moving_average_features'] = pd.Series(moving_avg_np_array)

# for avg in moving_averages(map(float, feature_df.values[:10]), 2):
#     print(avg)

# print(type(norm_train_df))
anomalies_np_array = explain_anomalies(norm_feature_np_ndarray, 10)

#print("***********")
#print(anomalies_np_array)
#print("***********")


anomalies_rolling_std_np_array = explain_anomalies_rolling_std(norm_feature_np_ndarray, 10)

#print("***********")
#print(anomalies_rolling_std_np_array)
#print("***********")

#print(train[:10].to_string())

# This function is responsible for displaying how the function performs on the given dataset.


def plot_results(x, y, window_size, sigma_value=1,
                 text_xlabel="X Axis", text_ylabel="Y Axis", applying_rolling_std=False):
    """ Helps in generating the plot and flagging the anamolies.
        Supports both moving and stationary standard deviation. Use the 'applying_rolling_std' to switch
        between the two.
    Args:
    -----
        x (pandas.Series): dependent variable
        y (pandas.Series): independent variable
        window_size (int): rolling window size
        sigma_value (int): value for standard deviation
        text_xlabel (str): label for annotating the X Axis
        text_ylabel (str): label for annotatin the Y Axis
        applying_rolling_std (boolean): True/False for using rolling vs stationary standard deviation
    """
    plt.figure(figsize=(15, 8))
    plt.plot(x, y, "k.")
    y_av = moving_average(y, window_size)
    plt.plot(x, y_av, color='green')
    plt.xlim(0, 1000)
    plt.xlabel(text_xlabel)
    plt.ylabel(text_ylabel)

    # Query for the anomalies and plot the same
    global events
    events = {}
    if applying_rolling_std:
        events = explain_anomalies_rolling_std(y, window_size=window_size, sigma=sigma_value)
    else:
        events = explain_anomalies(y, window_size=window_size, sigma=sigma_value)

    x_anomaly = np.fromiter(events['anomalies_dict'].keys(), dtype=int, count=len(events['anomalies_dict']))
    y_anomaly = np.fromiter(events['anomalies_dict'].values(), dtype=float,
                            count=len(events['anomalies_dict']))
    plt.plot(x_anomaly, y_anomaly, "r*", markersize=12)

    # add grid and lines and enable the plot
    plt.grid(True)
    plt.show()


x = train['minute']
Y = train[['normalized_vehicle_speed', 'normalized_engine_speed', 'normalized_tire_pressure']]

#print(Y.shape)

pca = PCA(n_components=1)

y_pca = pca.fit_transform(Y)

print(y_pca[:2])

# get_numeric_data = FunctionTransformer(lambda x: x[[0,1,2]], validate=False)
#
# pipeline = Pipeline([
#     ('features', get_numeric_data
#     )
# ]
# )
#
# featureUnion = FeatureUnion([('numeric_features', pipeline)])
# print(featureUnion)
#
# Y_tranformed = featureUnion.fit_transform(normalize_Y)
#
# print(Y_tranformed)
#
# print(pd.DataFrame(Y_tranformed).shape)

# plot_results(x, y=y_pca, window_size=10, text_xlabel='Minute', sigma_value=3, text_ylabel='Features',
#              applying_rolling_std=True)
#
# events = explain_anomalies(y_pca, window_size=5, sigma=3)

# Display the anomaly dict
# print("Information about the anomalies model:{}".format(events))

# Detect outliers using IsolationForest

data = train  #[['vehicle_speed',  'engine_speed',  'tire_pressure']]

print(data[:10].to_string())

class ExNode:
    def __init__(self,size):
        self.size=size

class InNode:
    def __init__(self,left,right,splitAtt,splitVal):
        self.left=left
        self.right=right
        self.splitAtt=splitAtt
        self.splitVal=splitVal

def iForest(X,noOfTrees,sampleSize):
    forest=[]
    hlim=math.ceil(math.log(sampleSize,2))
    for i in range(noOfTrees):
        X_train=X.sample(sampleSize)
        forest.append(iTree(X_train,0,hlim))
    return forest

def iTree(X,currHeight,hlim):
    if currHeight>=hlim or len(X)<=1:
        return ExNode(len(X))
    else:
        Q=X.columns
        q=random.choice(Q)
        p=random.choice(X[q].unique())
        X_l=X[X[q]<p]
        X_r=X[X[q]>=p]
        return InNode(iTree(X_l,currHeight+1,hlim),iTree(X_r,currHeight+1,hlim),q,p)


def pathLength(x,Tree,currHeight):
    if isinstance(Tree,ExNode):
        return currHeight
    a=Tree.splitAtt
    if x[a]<Tree.splitVal:
        return pathLength(x,Tree.left,currHeight+1)
    else:
        return pathLength(x,Tree.right,currHeight+1)

sampleSize=10000
ifor=iForest(data.sample(100000, replace=True),10,sampleSize) ##Forest of 10 trees

posLenLst=[]
negLenLst=[]

for sim in range(1000):
    ind=random.choice(data[is_anomaly_df==1].index)
    for tree in ifor:
        posLenLst.append(pathLength(data.iloc[ind],tree,0))

    ind=random.choice(data[is_anomaly_df==0].index)
    for tree in ifor:
        negLenLst.append(pathLength(data.iloc[ind],tree,0))


bins = np.linspace(0,math.ceil(math.log(sampleSize,2)), math.ceil(math.log(sampleSize,2)))

plt.figure(figsize=(12,8))
plt.hist(posLenLst, bins, alpha=0.5, label='Anomaly')
plt.hist(negLenLst, bins, alpha=0.5, label='Normal')
plt.xlabel('Path Length')
plt.ylabel('Frequency')
plt.legend(loc='upper left')


from sklearn.ensemble import IsolationForest

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(data, is_anomaly_df, test_size=0.3, random_state=42)

# print(X_train[:10].to_string())
# print(X_test[:10].to_string())
# print(y_train[:10].to_string())
# print(y_test[:10].to_string())


def train(X, clf, ensembleSize=5, sampleSize=10000):
    mdlLst=[]
    for n in range(ensembleSize):
        X = data.sample(sampleSize, replace=True)
        clf.fit(X)
        mdlLst.append(clf)
    return mdlLst


def predict(X,mdlLst):
    y_pred=np.zeros(X.shape[0])
    for clf in mdlLst:
        y_pred=np.add(y_pred, clf.decision_function(X).reshape(X.shape[0],))
    y_pred=(y_pred*1.0)/len(mdlLst)
    return y_pred


from sklearn.metrics import roc_auc_score
from sklearn.metrics import confusion_matrix, f1_score

alg = IsolationForest(n_estimators=100, max_samples='auto', contamination=0.01,
                      max_features=1.0, bootstrap=False, n_jobs=-1, random_state=42, verbose=0,behaviour="new")

if_mdlLst = train(X_train, alg)

if_y_pred=predict(X_test, if_mdlLst)
if_y_pred=1-if_y_pred

#Creating class labels based on decision function
if_y_pred_class=if_y_pred.copy()
if_y_pred_class[if_y_pred >= np.percentile(if_y_pred,95)] = 1
if_y_pred_class[if_y_pred < np.percentile(if_y_pred,95)] = 0

roc = roc_auc_score(y_test, if_y_pred_class)

print("******")
print(roc)
print("******")

f1 = f1_score(y_test, if_y_pred_class)

print("******")
print(f1)
print("******")

if_cm=confusion_matrix(y_test, if_y_pred_class)


df_cm = pd.DataFrame(if_cm,
                     ['True Normal','True Anomaly'],['Pred Normal','Pred Anomaly'])
print(df_cm[:10].to_string())

plt.figure(figsize = (8, 4))
sn.set(font_scale=1.4) #for label size
sn.heatmap(df_cm, annot=True,annot_kws={"size": 16}, fmt='g') # font size

#Using Kmeans

from sklearn.cluster import KMeans

kmeans = KMeans(n_clusters=8, random_state=42,n_jobs=-1).fit(X_train)

X_test_clusters=kmeans.predict(X_test)
X_test_clusters_centers=kmeans.cluster_centers_
dist = [np.linalg.norm(x-y) for x,y in zip(X_test.values,X_test_clusters_centers[X_test_clusters])]

km_y_pred=np.array(dist)
km_y_pred[dist>=np.percentile(dist,95)]=1
km_y_pred[dist<np.percentile(dist,95)]=0

kmeanroc = roc_auc_score(y_test, km_y_pred)

print("******")
print(kmeanroc)
print("******")


kmeanf1 = f1_score(y_test, km_y_pred)

print("******")
print(kmeanf1)
print("******")


km_cm=confusion_matrix(y_test, km_y_pred)

df_cm = pd.DataFrame(km_cm,
                     ['True Normal','True Anomaly'],['Pred Normal','Pred Anomaly'])
plt.figure(figsize = (8,4))
sn.set(font_scale=1.4)#for label size
sn.heatmap(df_cm, annot=True,annot_kws={"size": 16},fmt='g')# font size

plt.show()