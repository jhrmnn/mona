#!/usr/bin/env python
import sys
import cPickle as pickle

with open(sys.argv[1], 'rb') as f:
    results = pickle.load(f)

with open('results.txt', 'w') as f:
    f.write('%s\n' % {r.info['base']: r.data for r in results})
