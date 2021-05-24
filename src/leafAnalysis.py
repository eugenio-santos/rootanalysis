

import timeit
import csv
from datetime import datetime


def leaf_analysis(cur, tables, anomaly=True, num_leafs=1):
    results = open("all_children_gastos_train_result.txt", "w", newline='')
    now = datetime.now().strftime("%Y%m%d_%H")

    for table in tables:
        # get root keys
        query = 'SELECT * FROM {} WHERE RelationType = "ROOT" GROUP BY PrimaryKey'.format(
            table)

        cur.execute(query)

        rootKeys = []
        for c in cur:
            rootKeys.append(c[0])

        leafs = []
        for rootKey in rootKeys:
            res_leafs = get_leafs(cur, rootKey, table, anomaly, num_leafs)

            # fazes a analise caso seja %
            leafs += res_leafs

        res_txt = open('./res/'+table+'_'+now+'.txt', 'w')

        # metrics
        correct_leafs = 0
        total_leafs = len(leafs)
        for leaf in leafs:
            if leaf[3] == '1':
                correct_leafs += 1
        if total_leafs != 0:
            correct_percent = (correct_leafs/total_leafs) * 100

        print('Table: '+table)
        print('Anomalys :'+str(anomaly))
        print('Num of Leafs Limit: '+str(num_leafs))
        print('Root keys: '+rootKeys.__str__())
        print('Total Leafs: '+str(total_leafs))
        print('Correct Leafs'+str(correct_leafs) +
              ' Acuracy: ' + str(correct_percent))

        res_txt.write('Table: '+table+'\n')
        res_txt.write('Anomalys :'+str(anomaly)+'\n')
        res_txt.write('Num of Leafs Limit: '+str(num_leafs)+'\n')
        res_txt.write('Root keys: '+rootKeys.__str__()+'\n')
        res_txt.write('Total Leafs: '+str(total_leafs)+'\n')
        res_txt.write('Correct Leafs'+str(correct_leafs) +
                      ' Acuracy: ' + str(correct_percent)+'\n')

        # write results to csv
        res_csv = open('./res/'+table+'_'+now+'.csv', 'w', newline='')

        csv_writer = csv.writer(res_csv, dialect='excel')

        csv_writer.writerow(
            ('GroupKey', 'PrimaryKey', 'Date', 'Value',  'anomaly'))
        csv_writer.writerows(leafs)

        res_txt.close()
        res_csv.close()

        #print(count, correct, correct/count * 100)

    return 0


def get_leafs(cur, key, table, anomaly, num_leafs):
    # selcionar as datas
    query = 'SELECT date FROM {} WHERE PrimaryKey ="{}" and value != 0'.format(
        table, key)
    if anomaly:
        query = query + ' and anomaly = 1'
    cur.execute(query)
    dates = []

    for d in cur:
        dates.append(d[0])

    leafs = []
    for date in dates:
        query = "SELECT groupkey, primarykey, date, value, anomaly FROM ( SELECT * FROM {0} WHERE {0}.PrimaryKey NOT IN ( SELECT {0}.RelationKey FROM {0} WHERE {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AND {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AS A WHERE A.GroupKey = '{1}' AND A.Date = '{2}' AND A.relationkey != '' ORDER BY VALUE DESC limit {3}".format(
            table, key, date, num_leafs)

        cur.execute(query)

        leafs += cur.fetchall()

    return leafs
