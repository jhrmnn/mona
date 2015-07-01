#!/usr/bin/env python
import dispatcher

results = dispatcher.fetch()
with open('results.txt', 'w') as f:
    f.write('%s\n' % {r.info['base']: r.data for r in results})
