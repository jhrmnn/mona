from pathlib import Path
import shutil
import geomlib
import os


def prepare(path, task):
    path = Path(path)
    path.mkdir(parents=True)
    if 'geom' in task:
        g = task['geom']
    elif Path('geometry.in').is_file():
        g = geomlib.readfile('geometry.in', 'fhiaims')
    g.write(path/'geometry.in', 'fhiaims')
    species = set((a.number, a.symbol) for a in g.atoms)
    with Path('control.in').open() as f:
        template = f.read()
    with Path('basis').open() as f:
        basis = f.read().strip()
    with Path('aims').open() as f:
        aims = f.read().strip()
    basisroot = Path(os.environ['AIMSROOT'])/basis
    with (path/'control.in').open('w') as f:
        f.write(template % task)
        for s in species:
            f.write(u'\n')
            with (basisroot/('%02i_%s_default' % s)).open() as fspecie:
                f.write(fspecie.read())
    try:
        aimsbin = next(Path(os.environ['AIMSROOT']).glob(aims))
    except StopIteration:
        raise Exception('Cannot find binary %s' % aims)
    Path(path/'aims').symlink_to(aimsbin)
    shutil.copy('run_aims.sh', str(path/'run'))
    os.system('chmod +x %s' % (path/'run'))
