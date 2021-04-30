import mariadb
import sys
import timeit

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


root = 'A1'
date = 201701
rootValue = 899658.4599999995

print('starting for', root, date, rootValue)
start = timeit.default_timer()
leafCandidates = []

cur.execute("SELECT * FROM ( SELECT * FROM gastos_trial WHERE gastos_trial.PrimaryKey NOT IN ( SELECT gastos_trial.RelationKey FROM gastos_trial WHERE gastos_trial.GroupKey = 'A1' AND gastos_trial.Date = 201701) AND gastos_trial.GroupKey = 'A1' AND gastos_trial.Date = 201701) AS A WHERE A.GroupKey = 'A1' AND A.Date = 201701 AND A.relationkey != '' ORDER BY VALUE DESC")

for c in cur:
    if ((c[5]*100)/rootValue > 50):
        leafCandidates.append(c)

end = timeit.default_timer()
print('finishe in: ', end-start)
print(leafCandidates)