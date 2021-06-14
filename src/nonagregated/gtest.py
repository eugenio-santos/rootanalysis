import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.tsa.stattools as st
import seaborn as sn
from statsmodels.tsa.stattools import kpss
from statsmodels.tsa.stattools import adfuller
import warnings


def main(argv):
    statio = False
    thresholds = range(12, 18)
    num_lags = 15

    df = pd.read_csv(
        'C:/Users/1evsa/Desktop/M/proj/rootanalysis/dados/Mock_dataset/MockDataset_train.csv')

    df['DerivedFlag'] = df['DerivedFlag'].fillna(0)  # retirar nan da coluna
    df['Date'] = df['Date'].str.replace('-', '')
    df = df.astype({'DerivedFlag': 'int32'})

    # data frames to save the best results from granger causality tests
    df_ssrf = pd.DataFrame()

    groups = pd.unique(df['GroupKey'])
    # groups = groups[:1]
    for group in groups:
        print(group)
        gr = df[df['GroupKey'] == group]
        pks = pd.unique(gr['PrimaryKey'])
        root = df[df['PrimaryKey'] == group][['Date', 'Value']]

        if statio:
            root['Value'] = stationarity_trans(root['Value'])

        _ssrf = {}

        pks = pks[1:]  # remove A1(root) form keys
        for key in pks:
            flag = df.loc[(df['PrimaryKey'] == key), 'DerivedFlag'].values[0]
            rel = df[df['PrimaryKey'] == key][['Date', 'Value']]

            if statio:
                root['Value'] = stationarity_trans(root['Value'])

            VAR = pd.merge(root, rel, how='inner', on='Date')

            gc = st.grangercausalitytests(
                VAR[['Value_x', 'Value_y']], maxlag=num_lags, verbose=False)

            ssrf = {'score': 0, 'lag': 0, 'flag': flag, 'prediction': 0}

            for lag, item in gc.items():
                if item[0]['ssr_ftest'][0] > ssrf['score']:
                    ssrf['score'] = item[0]['ssr_ftest'][0]
                    ssrf['lag'] = lag

            _ssrf[key] = ssrf

        df_ssrf = pd.concat(
            [df_ssrf, pd.DataFrame.from_dict(_ssrf, orient='index')])

    tests = []
    tests += (conf_m_analysis(df_ssrf, thresholds, 'ssr_ftest'))

    t = pd.DataFrame(tests)
    print(t.sort_values(by=['f1-score'], ascending=False))

    # print(t)
    # for _ in tests:
    #    print(_['f1-score'])
    #    print(_['conf_m'])

    # print(t.sort_values(by=['acc'], ascending=False))


def conf_m_analysis(df, thresholds, m):
    metrics = []
    for threshold in thresholds:
        df['prediction'] = df['prediction'] = 0
        df.loc[df['score'] > threshold, ['prediction']] = 1
        conf_m = pd.crosstab(df['flag'], df['prediction'])

        T = (len(df))
        P = len(df[df['flag'] == 1])
        N = T - P
        TN = conf_m[0][0]
        FP = conf_m[0][1]
        FN = conf_m[1][0]
        TP = conf_m[1][1]

        f1 = f1_score(TP, FP, FN)
        acc = accuracy(TP, TN, FP, FN)
        sen = sensitivty(TP, P)
        spe = specificity(TN, N)
        kappa = cohens_kappa(TP, TN, FP, FN, T)

        metrics.append({'test': m, 'threshold': threshold, 'f1-score': f1,
                        'acc':  acc, 'sen': sen, 'spe': spe, 'kappa': kappa})

        # draw heat map
        # conf_m = conf_m.apply(lambda r: r/r.sum(), axis=1)  # percentagens
        # sn.heatmap(conf_m, annot=True, fmt='.2f')
        # plt.show()

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


# test if series is stationary and diferrences it a maximum of 5 times
def stationarity_trans(series):
    i = 0
    while adfuller(series, autolag='AIC')[1] < 0.05 and i < 5:
        series = differencing(series)
        series = series.dropna()
        i += 1
    return series


def differencing(timeseries):
    return timeseries - timeseries.shift(1)


if __name__ == "__main__":
    main(sys.argv[1:])
