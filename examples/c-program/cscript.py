import shutil
from pathlib import Path
from caflib import Task


class CompileTask(Task):
    def __init__(self, src, **kwargs):
        self.src = Path(src)
        super().__init__(
            command=f'gcc -c {self.src}',
            inputs=[self.src],
            **kwargs
        )

    @property
    def objfile(self):
        return self.outputs[f'{self.src.stem}.o']


class LinkTask(Task):
    def __init__(self, name, objfiles, **kwargs):
        self.name = name
        super().__init__(
            command=f'gcc -o {self.name} *.o',
            inputs=[(f.name, (f'_{f.name}_', f)) for f in objfiles],
            **kwargs
        )

    @property
    def app(self):
        return self.outputs[self.name].path if self.finished else None


def run(ctx):
    cobjs = [ctx(src=src, klass=CompileTask).objfile for src in Path().glob('*.c')]
    return ctx(name='myapp', objfiles=cobjs, klass=LinkTask, target='program').app


if __name__ == '__main__':
    from caflib import Cellar, Context

    cellar = Cellar('.caf')
    ctx = Context('.', cellar)
    shutil.copy(run(ctx), 'myapp')
