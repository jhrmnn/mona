#!/usr/bin/env python
import os

runner = u'''\
#!/bin/bash
sleep 3
expr `cat base.in` + `cat add.in`
'''


def prepare(path, task):
    os.system('cp add.in %s/' % path)
    with (path/'base.in').open('w') as f:
        f.write(u'%s\n' % task['base'])
    with (path/'run').open('w') as f:
        f.write(runner)
    os.system('chmod +x %s' % (path/'run'))
