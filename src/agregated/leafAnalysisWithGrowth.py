import timeit
import csv
import numpy as np
from datetime import datetime
import pandas as pd
from datetime import datetime, timedelta


def leaf_analysis(cur, tables, anomaly=True, num_leafs=1, percentage=0, growth_rate=0):
    #now = datetime.now().strftime("%Y%m%d_%H")
    #is_percent = bool(percentage)
    metrics = {}
    for table in tables:
        # get root keys
        query = 'SELECT * FROM {} WHERE RelationType = "ROOT" GROUP BY PrimaryKey'.format(
            table)

        cur.execute(query)

        rootKeys = []
        for c in cur:
            rootKeys.append(c[0])

        leafs = pd.DataFrame()
        for rootKey in rootKeys:
            res_leafs = get_leafs(cur, rootKey, table,
                                  anomaly, num_leafs, percentage, growth_rate)

            leafs = pd.concat([leafs, res_leafs])

        metrics[table] = conf_m_analysis(leafs, table)

    return metrics


# gets a table and a key and returns the leafs for each date in the series
def get_leafs(cur, key, table, anomaly, num_leafs, percentage, growth_rate):

    # selcionar as datas, sem valores = 0
    query = 'SELECT date, abs(value) FROM {} WHERE PrimaryKey ="{}" and value != 0'.format(
        table, key)
    if anomaly:
        query = query + ' and anomaly = 1'
    cur.execute(query)

    dates = []

    for d in cur:
        dates.append(d)

    leafs = pd.DataFrame()
    for date_value in dates:
        date = date_value[0]
        # calculate the previous date
        y_, m_ = date.split('-')
        prev_date = datetime(
            int(y_), int(m_), 1) - timedelta(days=1)
        s_prev_date = prev_date.strftime('%Y-%m')

        root_value = date_value[1]

        query = "SELECT groupkey, primarykey, date, abs(value), anomaly, 0 AS prediction FROM ( SELECT * FROM {0} WHERE {0}.PrimaryKey NOT IN ( SELECT {0}.RelationKey FROM {0} WHERE {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AND {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AS A WHERE A.GroupKey = '{1}' AND A.Date = '{2}' AND A.relationkey != '' ORDER BY VALUE DESC".format(
            table, key, date)

        cur.execute(query)

        df = pd.DataFrame(cur.fetchall(), columns=['GroupKey', 'PrimaryKey',
                                                   'Date', 'Value',  'anomaly', 'prediction'])
        probable_causes = pd.DataFrame()
        # percentage analysis
        if percentage != 0:
            # val = 503282.8899999998/899658.4599999995
            df['percentage'] = df['Value']/root_value
            # se val > percent then  ['prediction']] = 1
            probable_causes = df.loc[df['percentage']
                                     > percentage][['PrimaryKey', 'Value']]
            for l_key, val in probable_causes.values:
                if evaluate_leafs_last_month(cur,
                                             table, l_key, s_prev_date, val, growth_rate):
                    df.loc[df['PrimaryKey'] == l_key, ['prediction']] = 1

        # number of leafs
        else:
            df.loc[df.head(int(num_leafs)).index, ['prediction']] = 1

        leafs = pd.concat([leafs, df])

    return leafs


def evaluate_leafs_last_month(cur, table, leaf, date, value, growth_rate):
    query = 'SELECT * FROM {0} where primaryKey = "{1}" and date = "{2}"'.format(
        table, leaf, date)
    cur.execute(query)

    if cur.rowcount != 0:
        prev_val = cur.fetchone()[5]
        if prev_val and abs(value/prev_val) > growth_rate:
            return True
        else:
            return False
    return True


# all this functions should go on a separate module maybe
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

    metrics = ({'table': m, 'f1-score': f1, 'acc':  acc, 'sen': sen,
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
