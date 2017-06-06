from caflib.Configure import function_task


@function_task
def process(output):
    with open(output) as f:
        return int(f.read())+3


def run(ctx):
    results = {}
    for i in range(5):
        task_raw = ctx(
            command=f'sleep 1 && echo {2*i}'
        )
        task_process = ctx(
            command=f'expr $(cat output) + $(cat addition)',
            inputs=[
                ('output', ('raw', task_raw.outputs['run.out'])),
                'addition'
            ]
        )
        task_result = process(
            ('calc', task_process.outputs['run.out']),
            target=f'{i}',
            ctx=ctx
        )
        if task_result.finished:
            results[i] = task_result.result
    return results


if __name__ == '__main__':
    from caflib import Cellar, Context

    cellar = Cellar('.caf')
    ctx = Context('.', cellar)
    print(run(ctx))
