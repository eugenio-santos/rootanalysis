import csv as csv
import numpy as np
import matplotlib.pyplot as plt
import math
from collections import Counter


def load_csv(filename):
    dataset = list()
    with open(filename, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if not row:
                continue
            dataset.append(row)
    return dataset


data = load_csv(
    'C:/Users/1evsa/Desktop/M/proj/rootanalysis/dados/Datasets_mmc/Gastos_labeled/Dataset1_train.csv')

# print(data[0])
# print(data[1])
N = 200
V = np.zeros(20)
E = np.zeros([20, 3])
nV = 0
nE = 0
tabular = (nV, V, nE, E)
selected_anomaly = []
causal_chain = []
count_anomaly = np.zeros(N)
for node in data:
    if(node[8] == '1'):
        selected_anomaly.append(node)
        # print(node[1],node[2],node[3],node[4],node[5],node[6],node[7])

# print(selected_anomaly)
for node in selected_anomaly:
    str1 = node[1]
    str1 = '0'+''.join(filter(lambda i: i.isdigit(), str1))
    index1 = int(str1)
    str2 = node[2]
    str2 = '0'+''.join(filter(lambda i: i.isdigit(), str2))
    index2 = int(str2)
    count_anomaly[index1] += 1
    causal_chain.append(str2+'->'+str1)


print(count_anomaly)
causal_count = Counter(causal_chain)
for node in causal_count:
    print(node, causal_count[node])
