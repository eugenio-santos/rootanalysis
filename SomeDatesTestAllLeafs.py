import mariadb
import sys
import timeit

countAnalysis = 0

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user="root",
        password="root",
        host="127.0.0.1",
        port=3306,
        database="proj"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

cur = conn.cursor(buffered=True)


def rootAnalysis(key, date, table, f):
    global countAnalysis
    # selcionar as datas e valores de uma serie
    query = 'SELECT Date, Value FROM {} WHERE PrimaryKey ="{}" and date="{}"'.format(
        table, key, date)
    cur.execute(query)

    datesNvalues = []
    # ignoring 0 values for now
    for d in cur:
        if(d[1] != 0):
            datesNvalues.append(d)

    total = 0

    for dateNvalue in datesNvalues:
        date = dateNvalue[0]
        rootValue = dateNvalue[1]
        start = timeit.default_timer()
        leafCandidates = []

        query = "SELECT * FROM ( SELECT * FROM {0} WHERE {0}.PrimaryKey NOT IN ( SELECT {0}.RelationKey FROM {0} WHERE {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AND {0}.GroupKey = '{1}' AND {0}.Date = '{2}') AS A WHERE A.GroupKey = '{1}' AND A.Date = '{2}' AND A.relationkey != '' ORDER BY VALUE DESC".format(table, key, date)
        cur.execute(query)

        for c in cur:
            countAnalysis += 1
            if ((c[5]*100)/rootValue > 10):
                leafCandidates.append(c)
            #else:
                #break

        end = timeit.default_timer()
        total += end-start
        f.write('ROOT: {}, date: {}, $: {}, time:{}\n'.format(
            key, date, rootValue, end-start))

        f.write("leaf candidates: ")
        for l in leafCandidates:
            f.writelines('{}, '.format(l[1]))
        f.writelines("\n")
    f.write('###########\nAvg for Key: {} = {} on {} entries. \n###########\n'.format(
        key, total/len(datesNvalues), len(datesNvalues)))

    return total, len(datesNvalues)


#TABLE = 'gastos_train'

# results file
f = open("dates_to_test.txt", "w")

#query = 'SELECT * FROM {} WHERE RelationType = "ROOT" GROUP BY PrimaryKey'.format(
    #TABLE)

#cur.execute(query)

rootKeys = [{"PK": "M1", "date": "2018-12", "table": "Gastos_test"},
{"PK": "L1", "date": "2017-01", "table": "Gastos_test"},
{"PK": "L1", "date": "2017-10", "table": "Gastos_test"},
{"PK": "L1", "date": "2019-05", "table": "Gastos_test"},
{"PK": "L1", "date": "2020-05", "table": "Gastos_test"},
{"PK": "M1", "date": "2016-12", "table": "Fornecimentos_test"},
{"PK": "M1", "date": "2018-01", "table": "Fornecimentos_test"},
{"PK": "K1", "date": "2016-10", "table": "Fornecimentos_test"},
{"PK": "K1", "date": "2018-06", "table": "Fornecimentos_test"},
{"PK": "L1", "date": "2017-01", "table": "Fornecimentos_test"},
{"PK": "L1", "date": "2017-10", "table": "Fornecimentos_test"},
{"PK": "M1", "date": "2016-12", "table": "Rendimentos_test"},
{"PK": "M1", "date": "2017-12", "table": "Rendimentos_test"},
{"PK": "K1", "date": "2018-06", "table": "Rendimentos_test"},
{"PK": "L1", "date": "2019-12", "table": "Rendimentos_test"},
{"PK": "L1", "date": "2020-12", "table": "Rendimentos_test"},]


print(rootKeys)

times = []
for rootKey in rootKeys:
    times.append(rootAnalysis(rootKey['PK'], rootKey['date'], rootKey['table'], f))


totalTime = 0
numSeries = 0
for t in times:
    totalTime += t[0]
    numSeries += t[1]

print('Total: ', totalTime,  'Avg: ', totalTime/numSeries,
      ' # Series: ', numSeries, 'Total Analysis: ', countAnalysis)


f.close()
