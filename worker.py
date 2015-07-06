#!/usr/bin/env python
import sys
import os
import glob
import time

nargs = len(sys.argv[1:])
prefix, myid = sys.argv[1:3]
scratch = sys.argv[3] if len(sys.argv[1:]) == 3 else None
os.chdir(prefix)
while True:
    tasks = glob.glob('*.start')
    if not tasks:
        break
    startname = tasks[0]
    basename = os.path.splitext(startname)[0]
    runname = basename + '.running.' + myid
    try:
        os.rename(startname, runname)
    except:
        continue
    if scratch:
        today = time.strftime('%y-%m-%d')
        rundir = os.path.join(scratch, today, myid, basename)
        os.makedirs(rundir)
        os.symlink(rundir, os.path.join(runname, 'rundir'))
    else:
        rundir = os.path.join(runname, 'rundir')
        os.makedirs(rundir)
    os.system('rsync -a --exclude=rundir ./%s/ %s' % (runname, rundir))
    os.system('cd %s && ./run >run.log 2>run.err' % rundir)
    os.rename(runname, basename + '.done')
