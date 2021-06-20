#!python

import csv  as csv
import numpy as np
import matplotlib.pyplot as plt
import math
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.stattools import adfuller

def load_csv(filename):
    dataset = list()
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if not row:
                continue
            dataset.append(row)
    return dataset

def plot_curve(Y):
    plt.plot(range(len(Y)), Y, marker='o')
    plt.xlabel('time')
    plt.ylabel('value')
    #plt.ylim([0,120])
    plt.show()
    return

data=load_csv('./Mock_dataset/MockDataset_train.csv')
npdata=np.array(data)
Y=npdata[1:51,5]
Y = Y.astype(np.float64)
Y1=Y[1:]-Y[:-1]
Y2=Y1[1:]-Y1[:-1]
#----------------- I(0) -------------
mY=np.mean(Y);
vY=np.cov(Y,Y)
print(mY,vY)
plot_curve(Y)
plot_acf(Y, lags=10)
plt.show()
plot_pacf(Y, lags=10)
plt.show()
result = adfuller(Y)
print('ADF Statistic: %f' % result[0])
print('p-value: %f' % result[1])
print('Critical Values:')
for key, value in result[4].items():
	print('\t%s: %.3f' % (key, value))
#----------------- I(1) -------------
'''
mY1=np.mean(Y1);
vY1=np.cov(Y1,Y1)
print(mY1,vY1)
plot_curve(Y1)
plot_acf(Y1, lags=10)
plt.show()
plot_pacf(Y1, lags=10)
plt.show()
# --- ADF analysis ---
result = adfuller(Y1)
print('ADF Statistic: %f' % result[0])
print('p-value: %f' % result[1])
print('Critical Values:')
for key, value in result[4].items():
	print('\t%s: %.3f' % (key, value))
'''
#----------------- I(2) -------------
'''
mY2=np.mean(Y2);
vY2=np.cov(Y2,Y2)
print(mY2,vY2)
plot_curve(Y2)
plot_acf(Y2, lags=10)
plt.show()
plot_pacf(Y2, lags=10)
plt.show()
# --- ADF analysis ---
result = adfuller(Y2)
print('ADF Statistic: %f' % result[0])
print('p-value: %f' % result[1])
print('Critical Values:')
for key, value in result[4].items():
	print('\t%s: %.3f' % (key, value))

'''
