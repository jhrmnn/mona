import sys
import os
import glob
import time

myid, prefix, scratch = sys.argv[1:]
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
    today = time.strftime('%y-%m-%d')
    rundir = os.path.join(scratch, today, myid, basename)
    os.makedirs(rundir)
    os.system('rsync -a ./%s/ %s' % (runname, rundir))
    os.symlink(rundir, os.path.join(runname, 'rundir'))
    os.system('cd %s && ./run' % rundir)
    os.rename(runname, basename + '.done')
