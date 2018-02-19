from pathlib import Path
from typing import Any
import asyncio
import shutil

from caf import Caf
from caf.cellar import Cellar
from caf.executors import DirBashExecutor

app = Caf()
app.init()
cellar = Cellar(app, hook=True)
dir_bash = DirBashExecutor(app, cellar)


@app.register_route('main')
async def main() -> Any:
    sources = list(Path().glob('*.c'))
    objs = [str(s.with_suffix('.o')) for s in sources]
    obj_tasks = await asyncio.gather(*(
        dir_bash.task(f'gcc -c {src}', [src], label=str(src)) for src in sources
    ))
    return (await dir_bash.task(f'gcc -o app *.o', [
        (obj, tsk[obj]) for tsk, obj in zip(obj_tasks, objs)
    ], label='link'))['app'].path


if __name__ == '__main__':
    with app.context(execution=True, readonly=False):
        shutil.copy(app.get_route('main'), 'app')
