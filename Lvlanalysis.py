import mariadb
import sys
import timeit

root = 'A1'
date = 201701
rootValue = 899658.4599999995

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


#query for children
#populate candiadtes
#select series
#repeat
print('starting for', root, date, rootValue)
start = timeit.default_timer()
candidates = []
newCAnd = []
leafCandidates = []

def queryChildren(parent, date):
    cur.execute('SELECT * FROM gastos_trial WHERE gastos_trial.RelationKey = ? AND gastos_trial.date = ?', (parent, date) )

    for s in cur:
        candidates.append(s)

def selectCandidates():
    for c in candidates:
        if ((c[5]*100)/rootValue > 50):
            cur.execute('SELECT * FROM gastos_trial WHERE gastos_trial.RelationKey = ? AND gastos_trial.date = ?', (c[1], c[4]) )    
            if cur.rowcount == 0:
                leafCandidates.append(c)
            else:
                for s in cur:
                    newCAnd.append(s)



queryChildren(root, date)

while len(candidates) != 0:
    selectCandidates()
    candidates = newCAnd
    newCAnd = []
end = timeit.default_timer()
print('finishe in: ', end-start)
print(leafCandidates)
