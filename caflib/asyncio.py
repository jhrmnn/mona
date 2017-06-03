from caflib.Cellar import State
from caflib.Configure import TaskNode

cellar = None
ctx = None
inputs = None


class VirtualFile:
    def __init__(self, hashid):
        self.hashid = hashid

    @property
    def path(self):
        return cellar.get_file(self.hashid)


class Task:
    def __init__(self, node):
        self.node = node

    @property
    def hashid(self):
        return TaskNode.hashes[self.node]

    @property
    def state(self):
        return cellar.get_state(self.hashid)

    @property
    def finished(self):
        return self.state == State.DONE

    @property
    def outputs(self):
        return {
            name: VirtualFile(hashid) for name, hashid
            in cellar.get_task(self.hashid)['outputs'].items()
        }


def get_task(target=None, children=None, **kwargs):
    taskgen = ctx.add_task(**kwargs)
    if children:
        for childname, (child, childlinks) in children.items():
            taskgen.node.add_child(child.node, childname, *childlinks)
    if target:
        targetgen = ctx.target(None)
        targetgen.node.set_task(taskgen.node, target)
    taskgen.node.task.process(ctx)
    taskgen.node.seal(inputs)
    return Task(taskgen.node)
