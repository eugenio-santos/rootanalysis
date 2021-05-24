import getopt
import json
import sys

print(sys.argv)

opts, args = getopt.getopt(
    sys.argv[1:],
    'hl:p:',
    ['help', 'local_path', 'parameter'],
)

print(opts)

for opt, arg in opts:
    if opt in ('-l', '--local_path'):
        list_arg = json.loads(arg)
        print(opt + ': ' + str(list_arg))
    if opt in ('-p', '--parameter'):
        print(opt + ': ' + arg)
