import timeit
import csv
from datetime import datetime
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np


def lvl_analysis(cur, tables, anomaly=True, percentage=0.1):
    metrics = {}
    for table in tables:
        query = 'SELECT * FROM {} WHERE RelationType = "ROOT" GROUP BY PrimaryKey'.format(
            table)

        cur.execute(query)

        root_keys = []
        for c in cur:
            root_keys.append(c[0])

        print(root_keys)

        leafs = pd.DataFrame(columns=['GroupKey', 'PrimaryKey',
                                      'Date', 'Value',  'anomaly'])
        all_leafs = pd.DataFrame(columns=['GroupKey', 'PrimaryKey',
                                          'Date', 'Value',  'anomaly'])
        for root_key in root_keys:
            res_leafs, res_all_leafs = tbl_lvl_analysis(cur, root_key, table,
                                                        anomaly, percentage)
            # print(res_leafs)
            leafs = pd.concat([leafs, pd.DataFrame(res_leafs, columns=[
                'GroupKey', 'PrimaryKey', 'Date', 'Value',  'anomaly'])])

            all_leafs = pd.concat([all_leafs, pd.DataFrame(res_all_leafs,
                                                           columns=['GroupKey', 'PrimaryKey', 'Date', 'Value',  'anomaly'])])

        leafs['anomaly'] = 1
        all_leafs = pd.merge(all_leafs, leafs, how='outer',
                             on=['PrimaryKey', 'Date'])
        all_leafs['anomaly_y'] = all_leafs['anomaly_y'].fillna(0)
        all_leafs = all_leafs.astype(
            {'anomaly_x': 'int32', 'anomaly_y': 'int32'})
        all_leafs = all_leafs.rename(columns={'anomaly_x': 'anomaly',
                                              'anomaly_y': 'prediction'})
        all_leafs = all_leafs.drop(columns=['GroupKey_y', 'Value_y'])

        metrics[table] = conf_m_analysis(all_leafs, table)

    return metrics


def tbl_lvl_analysis(cur, key, table, anomaly, percentage):
    def select_candidates():
        for c in candidates:
            if (c[3]/root_value > percentage):
                query = 'SELECT groupkey, primarykey, date, abs(VALUE), anomaly FROM {} WHERE RelationKey = "{}" AND date = "{}"'.format(
                    table, c[1], c[2])
                cur.execute(query)
                if cur.rowcount == 0:
                    leaf_candidates.append(c)
                else:
                    for s in cur:
                        new_cand.append(s)

    # selcionar as datas e valores de uma serie
    query = 'SELECT Date, abs(Value), anomaly FROM {} WHERE PrimaryKey ="{}"'.format(
        table, key)
    if anomaly:
        query = query + ' and anomaly = 1'
    cur.execute(query)

    dates_n_values = []
    # ignoring 0 values for now
    for d in cur:
        if(d[1] != 0):
            dates_n_values.append(d)
    leaf_candidates = []
    all_leafs = []
    for date_n_value in dates_n_values:
        date = date_n_value[0]
        root_value = date_n_value[1]
        candidates = []
        new_cand = []

        # get all possible leafs
        query = "SELECT groupkey, primarykey, date, abs(value), anomaly FROM ( SELECT * FROM {0} WHERE {0}.PrimaryKey NOT IN ( SELECT {0}.RelationKey FROM {0} WHERE {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AND {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AS A WHERE A.GroupKey = '{1}' AND A.Date = '{2}' AND A.relationkey != '' ".format(
            table, key, date)

        cur.execute(query)
        all_leafs += cur.fetchall()

        # set up the first search - gets the first children of the root
        query = 'SELECT groupkey, primarykey, date, abs(VALUE), anomaly FROM {} WHERE RelationKey = "{}"  AND date = "{}"'.format(
            table, key, date)

        cur.execute(query)
        candidates = cur.fetchall()

        # main loop
        while len(candidates) != 0:
            select_candidates()
            candidates = new_cand
            new_cand = []

    return leaf_candidates, all_leafs


def conf_m_analysis(df, m):

    conf_m = pd.crosstab(df['anomaly'], df['prediction'])
    T = (len(df))
    P = len(df[df['anomaly'] == 1])
    N = T - P
    TN = conf_m[0][0]
    FP = conf_m[0][1]
    FN = conf_m[1][0]
    TP = conf_m[1][1]

    acc = accuracy(TP, TN, FP, FN)
    sen = sensitivty(TP, P)
    spe = specificity(TN, N)
    kappa = cohens_kappa(TP, TN, FP, FN, T)
    f1 = f1_score(TP, FP, FN)
    metrics = ({'test': m, 'f1-score': f1, 'acc':  acc, 'sen': sen,
                'spe': spe, 'kappa': kappa, 'conf_m': conf_m})

    return metrics


def accuracy(TP, TN, FP, FN):
    return (TP+TN)/(TP+TN+FP+FN)


def sensitivty(TP, P):
    return TP/P


def specificity(TN, N):
    return TN/N


# random accuracy
def rand_acc(TP, TN, FP, FN, T):
    return ((TN+FP)*(TN+FN)+(FN+TP)*(FP+TP))/(T*T)


# https://www.standardwisdom.com/2011/12/29/confusion-matrix-another-single-value-metric-kappa-statistic/
def cohens_kappa(TP, TN, FP, FN, T):
    return 1-((1-accuracy(TP, TN, FP, FN)) / (1-rand_acc(TP, TN, FP, FN, T)))


def f1_score(TP, FP, FN):
    return TP/(TP+(FP+FN)/2)
