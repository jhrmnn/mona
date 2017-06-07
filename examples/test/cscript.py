from caflib.Configure import function_task


@function_task
def process(output):
    with open(output) as f:
        return int(f.read())+3


def run(ctx):
    task_raw = ctx(
        command=f'echo 101'
    )
    task_process = ctx(
        command=f'expr $(cat output) + $(cat addition) + $(cat symlink)',
        inputs=[
            ('output', ('raw', task_raw.outputs['run.out'])),
            'addition'
        ],
        symlinks=[
            ('symlink', 'addition')
        ]
    )
    task_result = process(
        ('calc', task_process.outputs['run.out']),
        target='main',
        ctx=ctx
    )
    if task_result.finished:
        return task_result.result


if __name__ == '__main__':
    from caflib import Cellar, Context

    cellar = Cellar('.caf')
    ctx = Context('.', cellar)
    print(run(ctx))
