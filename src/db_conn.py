import mariadb
import sys


def maraidb_conn(user, password, ip, port, db, buff=True):
    try:
        conn = mariadb.connect(
            user=user,
            password=password,
            host=ip,
            port=port,
            database=db
        )
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    cur = conn.cursor(buffered=buff)

    return cur
