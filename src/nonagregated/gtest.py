import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas.core.frame import DataFrame
import statsmodels.tsa.stattools as st
import seaborn as sn
from statsmodels.tsa.stattools import kpss
from statsmodels.tsa.stattools import adfuller
import warnings


def main(argv):
    statio = False
    is_statio = True
    if True:  # ignore warnings
        warnings.catch_warnings()
        warnings.filterwarnings("ignore")

    #file = 'C:/Users/1evsa/Desktop/M/proj/rootanalysis/dados/Public_dataset/PublicDataset_test.csv'
    file = 'C:/Users/1evsa/Desktop/M/proj/rootanalysis/dados/Mock_dataset/MockDataset_test.csv'

    threshold = 0.05
    num_lags = 9

    df = pd.read_csv(file)

    df['Date'] = df['Date'].str.replace('-', '')

    # data frames to save the best results from granger causality tests
    df_ssrf = pd.DataFrame()

    groups = pd.unique(df['GroupKey'])
    for group in groups:
        print(group)
        gr = df[df['GroupKey'] == group]
        pks = pd.unique(gr['PrimaryKey'])
        root = df[df['PrimaryKey'] == group][['Date', 'Value']]

        if statio:
            is_statio, root['Value'] = stationarity_trans(root['Value'])
            if not is_statio:
                next

        _ssrf = {}

        pks = pks[1:]  # remove root form keys
        for key in pks:
            try:
                rel = df[df['PrimaryKey'] == key][['Date', 'Value']]

                if statio:
                    is_statio, rel['Value'] = stationarity_trans(rel['Value'])
                    if not is_statio:
                        next

                VAR = pd.merge(root, rel, how='inner', on='Date')
                VAR = VAR.dropna()

                gc = st.grangercausalitytests(
                    VAR[['Value_x', 'Value_y']], maxlag=num_lags, verbose=False)

                ssrf = {'group': group, 'key': key,
                        'score': 0, 'p-value': 0, 'lag': 0, 'prediction': 0}

                for lag, item in gc.items():
                    if item[0]['ssr_ftest'][0] > ssrf['score']:
                        ssrf['score'] = item[0]['ssr_ftest'][0]
                        ssrf['p-value'] = item[0]['ssr_ftest'][1]
                        ssrf['lag'] = lag
                _ssrf[key] = ssrf
            except Exception as ex:
                True
                # print('# EXCEPTION START #')
                # print(ex, ex.with_traceback)
                # print(group, key)
                # print('# EXCEPTION   END #')
        df_ssrf = pd.concat(
            [df_ssrf, pd.DataFrame.from_dict(_ssrf, orient='index')])
    df_ssrf.loc[df_ssrf['p-value'] < threshold, ['prediction']] = 1
    print(df_ssrf)

    df_ssrf[df_ssrf['prediction'] == 1].to_csv(
        'C:/Users/1evsa/Desktop/M/proj/rootanalysis/dados/mockD_test_results_nonstatio_p_0.05.csv', index=False)

# test if series is stationary and diferrences it a maximum of 5 times


def stationarity_trans(series):
    i = 0
    while adfuller(series, autolag='AIC')[1] > 0.05 and kpss(series, regression='c', nlags="auto")[1] < 0.05 and i < 5:
        series = differencing(series)
        series = series.dropna()
        i += 1
    if i >= 5:
        print('ignoring')
        return False, series
    else:
        return True, series


def differencing(timeseries):
    return timeseries - timeseries.shift(1)


if __name__ == "__main__":
    main(sys.argv[1:])
