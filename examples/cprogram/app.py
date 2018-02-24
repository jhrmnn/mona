from pathlib import Path
from typing import Any
import asyncio
import shutil

from caf import Caf
from caf.cellar import Cellar
from caf.executors import DirBashExecutor

app = Caf()
cellar = Cellar(app)
dir_bash = DirBashExecutor(app, cellar)


@app.route('main')
async def main() -> Any:
    sources = list(Path().glob('*.c'))
    objs = [str(s.with_suffix('.o')) for s in sources]
    obj_tasks = await asyncio.gather(*(
        dir_bash.task(f'gcc -c {src}', [src], label=str(src)) for src in sources
    ))
    return await dir_bash.task(f'gcc -o app *.o', [
        (obj, tsk[obj]) for tsk, obj in zip(obj_tasks, objs)
    ], label='link')


if __name__ == '__main__':
    with app.context(executing=True, readonly=False):
        shutil.copy(app.get('main')['app'].path, 'app')
