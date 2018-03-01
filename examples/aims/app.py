from math import nan
from pathlib import Path
from typing import List

from caf import Caf, Cellar, collect
from caf.executors import DirBashExecutor
from caf.Tools.aims import AimsTask
from caf.Tools.geomlib import Molecule

app = Caf()
cellar = Cellar(app)
aims = AimsTask(DirBashExecutor(app, cellar))


@app.route('main')
async def main() -> List[float]:
    dists = [3.3, 4, 5.2]
    return await collect(*(get_ene(dist) for dist in dists), nan)


async def get_ene(dist: float) -> float:
    outputs = await aims.task(
        aims='aims-master',
        tags={'xc': 'pbe'},
        geom=Molecule.from_coords(['Ar', 'Ar'], [(0, 0, 0), (dist, 0, 0)]),
        basis='light',
        label=str(dist),
    )
    return parse_ene(outputs['run.out'].path)


def parse_ene(path: Path) -> float:
    with path.open() as f:
        return float(
            next(l for l in f if 'Total energy uncorrected' in l).split()[5]
        )


if __name__ == '__main__':
    with app.context():
        print(app.get('main'))
