#!/usr/bin/python

import sys
import getopt
import json
import db_conn
import leafAnalysis


def main(argv):
    num_leafs = 1
    anomaly = True
    try:
        opts, args = getopt.getopt(argv, "hl:nt:", [])
    except getopt.GetoptError:
        print('test.py -t [table names] -l <number of leafs in query> -n')
        sys.exit(2)

    # config options
    for opt, arg in opts:
        if opt == '-h':
            print('-t [table names] -l <number of leafs in query> -n')
            sys.exit()
        elif opt in ("-l"):
            num_leafs = arg
        elif opt in ("-n"):
            anomaly = False

    tabels = []
    # exec options
    for opt, arg in opts:
        if opt == '-t':
            tabels = json.loads(arg)
            print(tabels)
            cur = db_conn.maraidb_conn(
                'root', 'root', '127.0.0.1', 3306, 'proj')
            leafAnalysis.leaf_analysis(
                cur, tabels, anomaly=anomaly, num_leafs=num_leafs)


if __name__ == "__main__":
    main(sys.argv[1:])
