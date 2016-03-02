# `caf` — Calculation framework

Caf is a distributed build system, inspired by [Waf](https://waf.io), [Git](https://git-scm.com) and [Homebrew](http://brew.sh) and written in Python 3.

Caf can be used as a replacement for Make

```python
def build(ctx):
    [ctx(features='gcc', src=src) + ctx.link(src) for src in glob('*.c')] \
        + ctx(features='gcc', program='main') @ ctx.target('app')
```

But it can easily manage also more complex workflows

```python
def build(ctx):
    [ctx(features='calculation', inp=inp) @ ctx.target('calculation', inp)
     + ctx.link('calc', 'calc.out')
     + ctx(features='postprocess')
     + ctx.link('data', 'data.json')
     for inp in glob('*.inp')] \
        + ctx(features='analysis') @ ctx.target('analysis')
```

It provides a comprehensive set of commands that can interact with the build process

```
Usage:
    caf init
    caf [init] build [--dry]
    caf [[init] build] work [TARGET...] [--depth N] [--limit N]
                            [--profile PROFILE [-j N] | [--id ID] [--dry]]
    caf work [--queue URL] [--profile PROFILE [-j N] | [--id ID] [--dry]]
    caf submit URL
    caf status
    caf list (profiles | remotes)
    caf list tasks [--finished | --stored]
    caf search [--contains PATTERN] [--older TIME]
    caf cmd CMD
    caf remote add URL [NAME]
    caf update REMOTE [--delete]
    caf check REMOTE [TARGET...]
    caf push REMOTE [TARGET...] [--dry]
    caf fetch REMOTE [TARGET...] [--dry]
    caf go REMOTE
    caf REMOTE [--] CMD...
    caf (pack | unpack | strip)
```

Caf is based on a data model in which

- each build task lives in its own directory
- tasks can have other tasks as dependencies and these are recorded as symlinks
- each task goes through two stages: preparation and execution
- in the preparation stage, any agent can modify the contents of the task's directory
- a task's preparation can depend on the results of the execution of its dependencies
- when preparation is finished, the contents of the task's directory are hashed and the task is stored by its hash, which uniquely defines the task across any machine
- the dependencies are hashed via their own hashes and hence a task is prepared only when all its dependencies are prepared
- the execution of a task is performed by running a single command, which is a part of the hashed contents, is deterministic, has no side effects beyond the task's directory and its results depend only on the task's hashed contents (which means also its dependencies, which are hashed)
- executed tasks can be exchanged between machines based purely on their hashes and disregarding how the task was prepared

In Caf, the above data model is implemented in a partially static and offline manner, in which

- the dependency tree is defined statically in a build script and is hence known prior to any preparations or executions
- the preparation of tasks is handled by `caf build`  which fully prepares all tasks within the dependency tree, the preparations of which do not depend on yet unexecuted dependencies
- the execution of tasks is handled by `caf work`, which dispatches independent workers communicating only via the file system, which execute prepared tasks
- as a result, if there are tasks whose preparation depends on the results of its dependencies, `caf build work` needs to be run several times to prepare and execute all tasks

## Documentation

For a provisional documentation, see [Read the Docs](http://readthedocs.org/docs/caf/en/latest/).
