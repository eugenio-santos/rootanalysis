import mariadb
import sys
import timeit

countAnalysis = 0

# Connect to MariaDB
try:
    conn = mariadb.connect(
        user="root",
        password="root",
        host="127.0.0.1",
        port=3306,
        database="proj"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB: {e}")
    sys.exit(1)

cur = conn.cursor(buffered=True)


def rootAnalysis(key, table, f): 
    def selectCandidates():
        for c in candidates:
            # processo de decisão, para já verifica se o valor é 
            # superior a uma certa percentagem do root
            global countAnalysis  
            countAnalysis += 1
            if ((c[5]*100)/rootValue > 10):
                query = 'SELECT * FROM {} WHERE RelationKey = "{}" AND date = "{}"'.format(table, c[1], c[4])
                cur.execute(query)
                if cur.rowcount == 0:
                    leafCandidates.append(c)
                else:
                    for s in cur:
                        newCAnd.append(s)

    # selcionar as datas e valores de uma serie
    query = 'SELECT Date, Value, anomaly FROM {} WHERE PrimaryKey ="{}"'.format(table, key)
    cur.execute(query)

    datesNvalues = []
    #ignoring 0 values for now
    for d in cur:
        if(d[1] != 0): 
            datesNvalues.append(d)
    total = 0
    for dateNvalue in datesNvalues:
        date = dateNvalue[0]
        rootValue = dateNvalue[1]
        anom = dateNvalue[2]
        start = timeit.default_timer()
        candidates = []
        newCAnd = []
        leafCandidates = []

        #set up the first search - gets the first children of the root
        query = 'SELECT * FROM {} WHERE RelationKey = "{}"  AND date = "{}"'.format(table, key, date)
        cur.execute(query)

        for s in cur:
            candidates.append(s)

        # main loop
        while len(candidates) != 0:
            selectCandidates()
            candidates = newCAnd
            newCAnd = []

        end = timeit.default_timer()
        total += end-start

        f.write('ROOT: {}, date: {}, $: {}, time:{}, a:{}\n'.format(key, date, rootValue, end-start, anom))
        f.write("leaf candidates: \n")
        for l in leafCandidates:
            f.writelines('{}, {}, {}, {}\n'.format(l[1], l[4], l[5], l[8]))

    f.write('###########\nAvg for Key: {} = {} on {} entries. \n###########\n'.format(key, total/len(datesNvalues), len(datesNvalues)))

    return total, len(datesNvalues)



TABLE = 'gastos_train'

#results file
f = open("gastos_train_result.txt", "w")

query = 'SELECT * FROM {} WHERE RelationType = "ROOT" GROUP BY PrimaryKey'.format(TABLE)

cur.execute(query)

rootKeys = []
for c in cur:
    rootKeys.append(c[0])

print(rootKeys)

times = []
for rootKey in rootKeys:
    times.append(rootAnalysis(rootKey, TABLE, f))

totalTime = 0
numSeries = 0
for t in times:
    totalTime += t[0]
    numSeries += t[1]

print('Total: ', totalTime,  'Avg: ', totalTime/numSeries, ' # Series: ', numSeries, 'Total Analysis: ', countAnalysis)

f.close()





