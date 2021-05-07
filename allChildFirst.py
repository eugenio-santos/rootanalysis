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


root = 'M1'
date = '2018-12'

rootValue = 899658.4599999995

print('starting for', root, date, rootValue)
start = timeit.default_timer()
leafCandidates = []

cur.execute("SELECT * FROM ( SELECT * FROM gastos_test WHERE gastos_test.PrimaryKey NOT IN ( SELECT gastos_test.RelationKey FROM gastos_test WHERE gastos_test.GroupKey = 'M1' AND gastos_test.Date = '2018-12') AND gastos_test.GroupKey = 'M1' AND gastos_test.Date = '2018-12') AS A WHERE A.GroupKey = 'M1' AND A.Date = '2018-12' AND A.relationkey != '' ORDER BY VALUE DESC")

for c in cur:
    if ((c[5]*100)/rootValue > 10):
        leafCandidates.append(c)

end = timeit.default_timer()
print('finishe in: ', end-start)
print(leafCandidates)