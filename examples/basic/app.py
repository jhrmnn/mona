from caf import Caf, Cellar, collect
from caf.executors import DirBashExecutor, DirPythonExecutor

app = Caf()
cellar = Cellar(app)
dir_bash = DirBashExecutor(app, cellar)
dir_python = DirPythonExecutor(app, cellar)


@dir_python.function_task
def process(output: str) -> int:
    with open(output) as f:
        return int(f.read())+3


@app.route('main')
async def main() -> int:
    task_raw = await dir_bash.task(f'echo 101', label='raw')
    task_process = await dir_bash.task(
        f'expr $(cat output) + $(cat addition) + $(cat symlink)',
        inputs=[('output', task_raw['run.out']), 'addition'],
        symlinks=[('symlink', 'addition')],
        label='calc'
    )
    return (await collect(process(task_process['run.out'], label='main')))[0]  # type: ignore


if __name__ == '__main__':
    with app.context():
        print(app.get('main'))
