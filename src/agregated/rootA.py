#!/usr/bin/python

import sys
import getopt
import json
import db_conn
import leafAnalysis
import lvlAnalysis
import timeit


def main(argv):
    num_leafs = 1
    anomaly = True
    percentage = 0
    tabels = []

    try:
        opts, args = getopt.getopt(argv, "hqlan:t:p:P:LR:", [])
    except getopt.GetoptError:
        print('test.py -t [table names] -l <number of leafs in query> -n')
        sys.exit(2)

    # config options
    for opt, arg in opts:
        if opt == '-h':
            print('-t [table names] -n <number of leafs in query> -a')
            sys.exit()
        elif opt in ("-t"):  # tables
            tabels = json.loads(arg)
            print(tabels)
        elif opt in ("-n"):
            num_leafs = int(arg)
        elif opt in ("-p"):
            percentage = float(arg)
        elif opt in ("-a"):
            anomaly = False

    # exec options
    for opt, arg in opts:
        # Leaf analysis either by % or rank
        if opt == '-q':
            print('query')
            cur = db_conn.maraidb_conn('root', 'root',
                                       '127.0.0.1', 3306, 'proj')
            metrics = leafAnalysis.leaf_analysis(cur, tabels, anomaly=anomaly,
                                                 num_leafs=num_leafs, percentage=percentage)
            for m in metrics:
                print(metrics[m]['table'], metrics[m]['f1-score'])
                print(metrics[m]['conf_m'])

        # Lvl analysis by %
        elif opt in ("-l"):
            print('by lvl')
            cur = db_conn.maraidb_conn('root', 'root',
                                       '127.0.0.1', 3306, 'proj')
            lvl_metrics = lvlAnalysis.lvl_analysis(cur, tabels, anomaly=anomaly,
                                                   percentage=percentage)
            print(lvl_metrics)

        # Leaf analysis searching for best % in an interval
        # usage [start, end, step]
        elif opt in ("-P"):
            print('Percentage benchmark')

            # create percentages to test
            p_arg = json.loads(arg)
            percentages = []
            percentages.append(p_arg[0])
            i = 0
            while percentages[i] < p_arg[1]:
                percentages.append(round(percentages[i]+p_arg[2], 2))
                i += 1

            cur = db_conn.maraidb_conn('root', 'root',
                                       '127.0.0.1', 3306, 'proj')
            for percent in percentages:
                print(percent)
                print(leafAnalysis.leaf_analysis(cur, tabels, anomaly=anomaly,
                                                 num_leafs=num_leafs, percentage=percent))

        # Leaf analysis searching for best Rank in an interval
        # usage [start, end, step]
        elif opt in ("-R"):
            print('Rank Benchmark')

            # create percentages to test
            ranks = json.loads(arg)

            cur = db_conn.maraidb_conn('root', 'root',
                                       '127.0.0.1', 3306, 'proj')
            for rank in range(ranks[0], ranks[1]+ranks[2], ranks[2]):
                print(rank)
                print(leafAnalysis.leaf_analysis(cur, tabels, anomaly=anomaly,
                                                 num_leafs=rank, percentage=0))

        # comparison Lvl vs Leaf
        elif opt in ("-L"):
            print('Lvl\'s vs Leafs')
            cur = db_conn.maraidb_conn('root', 'root',
                                       '127.0.0.1', 3306, 'proj')
            lvl_metrics = lvlAnalysis.lvl_analysis(cur, tabels, anomaly=anomaly,
                                                   percentage=percentage)
            print(lvl_metrics)
            leaf_metrics = leafAnalysis.leaf_analysis(cur, tabels, anomaly=anomaly,
                                                      num_leafs=num_leafs, percentage=percentage)
            print(leaf_metrics)


if __name__ == "__main__":
    main(sys.argv[1:])
