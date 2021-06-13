import timeit
import csv
from datetime import datetime
import pandas as pd


def leaf_analysis(cur, tables, anomaly=True, num_leafs=1, percentage=0):
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
                                  anomaly, num_leafs, percentage)

            leafs = pd.concat([leafs, res_leafs])

        # Data Frame
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            # print(leafs)
        # metrics
        metrics[table] = conf_m_analysis(leafs, table)

        #print('Table: '+table)
        #print('Anomalys :'+str(anomaly))
        #print('Num of Leafs Limit: '+str(num_leafs))
        #print('Root keys: '+rootKeys.__str__())
        #print('Total Leafs: '+str(len(leafs)))
        #print('Acuracy: ' + str(metrics['acc']))
        #print(conf_m_analysis(leafs, 'agregated'))

    return metrics


# gets a table and a key a returns the leafs for each date in the series
def get_leafs(cur, key, table, anomaly, num_leafs, percentage):

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
        root_value = date_value[1]

        query = "SELECT groupkey, primarykey, date, abs(value), anomaly, 0 AS prediction FROM ( SELECT * FROM {0} WHERE {0}.PrimaryKey NOT IN ( SELECT {0}.RelationKey FROM {0} WHERE {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AND {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AS A WHERE A.GroupKey = '{1}' AND A.Date = '{2}' AND A.relationkey != '' ORDER BY VALUE DESC".format(
            table, key, date)

        cur.execute(query)

        df = pd.DataFrame(cur.fetchall(), columns=['GroupKey', 'PrimaryKey',
                                                   'Date', 'Value',  'anomaly', 'prediction'])

        if percentage != 0:
            # percentage analysis
            df['percentage'] = df['Value']/root_value
            df.loc[df['percentage'] > percentage, ['prediction']] = 1
        else:
            # number of leafs
            df.loc[df.head(int(num_leafs)).index, ['prediction']] = 1

        leafs = pd.concat([leafs, df])

    return leafs


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
