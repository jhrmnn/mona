from pathlib import Path
import shutil
import geom
import os


def prepare(path, task):
    path = Path(path)
    path.mkdir(parents=True)
    if 'geom' in task:
        g = task['geom']
    elif Path('geometry.in').is_file():
        g = geom.readfile('fhiaims', 'geometry.in')
    g.write('fhiaims', str(path/'geometry.in'))
    species = set((a.number, a.symbol) for a in g.atoms)
    with Path('control.in').open() as f:
        template = f.read()
    with (path/'control.in').open('w') as f:
        f.write(template % task)
        for s in species:
            f.write(u'\n')
            with (Path('basis')/('%02i_%s_default' % s)).open() as fspecie:
                f.write(fspecie.read())
    Path(path/'aims').symlink_to(Path('aims').resolve())
    shutil.copy('run_aims.sh', str(path/'run'))
    os.system('chmod +x %s' % (path/'run'))
