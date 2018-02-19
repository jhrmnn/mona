from math import nan
from pathlib import Path
from typing import List

from caf import Caf
from caf.cellar import Cellar, collect
from caf.executors import DirBashExecutor
from caf.Tools.aims import AimsTask
from caf.Tools.geomlib import Molecule
from caf.scheduler import Scheduler

app = Caf()
cellar = Cellar(app, hook=True)
dir_bash = DirBashExecutor(app, cellar)
Scheduler(cellar, hook=True)
aims = AimsTask(dir_bash=dir_bash)


def parse_ene(path: Path) -> float:
    with open(path) as f:
        return float(
            next(l for l in f if 'Total energy uncorrected' in l).split()[5]
        )


async def get_ene(dist: float) -> float:
    outputs = await aims.task(
        aims='aims-master',
        basis='light',
        tags=dict(
            xc='pbe',
        ),
        geom=Molecule.from_coords(['Ar', 'Ar'], [
            (0, 0, 0),
            (dist, 0, 0),
        ]),
        label=str(dist),
    )
    return parse_ene(outputs['run.out'].path)


@app.route('main')
async def main() -> List[float]:
    dists = [3.3, 4, 5.2]
    return await collect((get_ene(dist) for dist in dists), nan)


if __name__ == '__main__':
    with app.context():
        print(app.get('main'))
