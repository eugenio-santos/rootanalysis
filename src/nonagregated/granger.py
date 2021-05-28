import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import statsmodels.tsa.stattools as st
import seaborn as sn


df = pd.read_csv(
    'C:/Users/1evsa/Desktop/M/proj/rootanalysis/dados/Mock_dataset/MockDataset_train.csv')

df['DerivedFlag'] = df['DerivedFlag'].fillna(0)  # retirar nan da coluna
df['Date'] = df['Date'].str.replace('-', '')
df = df.astype({'DerivedFlag': 'int32'})

groups = pd.unique(df['GroupKey'])


# data frames to save the best results from granger causality tests
df_ssrf = pd.DataFrame()
df_ssrchi = pd.DataFrame()
df_lrtest = pd.DataFrame()
df_parmf = pd.DataFrame()


for group in groups:
    gr = df[df['GroupKey'] == group]
    pks = pd.unique(gr['PrimaryKey'])
    root = df[df['PrimaryKey'] == group][['Date', 'Value']]

    _ssrf = {}
    _ssrchi = {}
    _lrtest = {}
    _parmf = {}

    num_lags = 3
    pks = pks[1:]  # remove A1(root) form keys
    for key in pks:
        flag = df.loc[(df['PrimaryKey'] == key), 'DerivedFlag'].values[0]
        rel = df[df['PrimaryKey'] == key][['Date', 'Value']]
        VAR = pd.merge(root, rel, how='outer', on='Date')

        gc = st.grangercausalitytests(
            VAR[['Value_x', 'Value_y']], maxlag=num_lags, verbose=False)

        ssrf = {'score': 0, 'lag': 0, 'flag': flag, 'prediction': 0}
        ssrchi = {'score': 0, 'lag': 0, 'flag': flag, 'prediction': 0}
        lrtest = {'score': 0, 'lag': 0, 'flag': flag, 'prediction': 0}
        parmf = {'score': 0, 'lag': 0, 'flag': flag, 'prediction': 0}

        for lag, item in gc.items():
            if item[0]['ssr_ftest'][0] > ssrf['score']:
                ssrf['score'] = item[0]['ssr_ftest'][0]
                ssrf['lag'] = lag
            if item[0]['ssr_chi2test'][0] > ssrchi['score']:
                ssrchi['score'] = item[0]['ssr_chi2test'][0]
                ssrchi['lag'] = lag
            if item[0]['lrtest'][0] > lrtest['score']:
                lrtest['score'] = item[0]['lrtest'][0]
                lrtest['lag'] = lag
            if item[0]['params_ftest'][0] > parmf['score']:
                parmf['score'] = item[0]['params_ftest'][0]
                parmf['lag'] = lag

        _ssrf[key] = ssrf
        _ssrchi[key] = ssrchi
        _lrtest[key] = lrtest
        _parmf[key] = parmf

    df_ssrf = pd.concat(
        [df_ssrf, pd.DataFrame.from_dict(_ssrf, orient='index')])
    df_ssrchi = pd.concat(
        [df_ssrchi, pd.DataFrame.from_dict(_ssrchi, orient='index')])
    df_lrtest = pd.concat(
        [df_lrtest, pd.DataFrame.from_dict(_lrtest, orient='index')])
    df_parmf = pd.concat(
        [df_parmf, pd.DataFrame.from_dict(_parmf, orient='index')])

print(df_ssrf)

threshold = 15

df_ssrf.loc[df_ssrf['score'] > threshold, ['prediction']] = 1

print(df_ssrf)

confusion_matrix = pd.crosstab(df_ssrf['flag'], df_ssrf['prediction'])

print(confusion_matrix)


# SHOW HEAT MAP DA MATRIZ DE CONFUSÃO
sn.heatmap(confusion_matrix, annot=True, fmt='.1f')
plt.show()
