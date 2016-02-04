# `caf` — Calculation framework

Caf is a distributed build system, inspired by [Waf](https://waf.io), [Git](https://git-scm.com) and [Homebrew](http://brew.sh) and written in Python 3. It is based on a data model in which

- each build task lives in its own directory
- tasks can have other tasks as dependencies and these are recorded as symlinks
- each task goes through two stages: preparation and execution
- in the preparation stage, any agent can modify the contents of the task's directory
- a task's preparation can depend on the results of the execution of its dependencies
- when preparation is finished, the contents of the task's directory are hashed and the task is stored by its hash, which uniquely defines the task across any machine
- the dependencies are hashed via their own hashes and hence a task is prepared only when all its dependencies are prepared
- the execution of a task is performed by running a single command, which is a part of the hashed contents, is deterministic, has no side effects beyond the task's directory and its results depend only on the task's hashed contents
- as a result, executed tasks can be exchanged between machines based purely on their hashes and disregarding how the task was prepared

In Caf, the above data model is implemented in a partially static and offline manner, in which

- the dependency tree is defined statically in a build script and is hence known prior to any preparations or executions
- the preparation of tasks is handled by `caf build`  which fully prepares all tasks within the dependency tree, the preparations of which do not depend on yet unexecuted dependencies
- the execution of tasks is handled by `caf work`, which dispatches independent workers communicating only via the file system which execute prepared tasks
- as a result, if there are tasks whose preparation depends on the results of its dependencies, `caf build work` needs to be run several times to prepare and execute all tasks

In the project directory, Caf organizes the builds and tasks in a following way:

- `_caf` is the repository of tasks
- `build` is the repository of builds, which are collections of targets, which are collections of tasks
- each run of `caf build` creates a timestamped batch in `_caf/Brewery` where all tasks defined in a build script are initialized
- in addition, all tasks that can be prepared are prepared and stored in `_caf/Cellar` akin to Git; if the task of the same hash is already stored in the cellar, it is discarded from the brewery and symlinked from the cellar
- `caf build` also creates a timestamped build directory in `build` where symlinks to tasks in the brewery or in the cellar are created in directories corresponding to targets
- `caf work` dispatches workers that execute tasks in the cellar

## Cscript

The dependency tree as well as the definition of the individual tasks and targets is defined in a build script named `csript` (calculation script), which is an equivalent of a Makefile for Make. A minimal Cscript, which does nothing, contains

``` python
def build(ctx):
    pass
```

The single argument `ctx` of the `build` function is a so-called build context which gives access to the API of Caf. Tasks are defined by calling the build context with keyword arguments (task attributes), the only mandatory attribute being `command`, which is the shell command that performs the execution of a task. A minimal task, which does nothing, is defined by

``` python
ctx(command='')
```

Other notable task attributes that might be present are:

- *files*: A list of filenames or 2-tuples of filenames, that are to be copied from project's directory to the task directory. If a tuple `t` is given, file `t[0]` in the project directory is copied to file `t[1]` in the task directory.
- *templates*: Similar to `files`, but the file is first processed and all instances of `{{ <task attribute> }}` are replaced.
- *features*: A list of functions or names of registered functions that are executed in the task directory after *files* and *templates* are applied. Features accept a single argument, the task object, and access to task attributes is given by `task.consume(attribute)`.

For all three attributes above, passing `[x]` is equivalent to passing `x`. Any other user attributes might be passed to `ctx`, and those can then be consumed by various features. An important rule is that after the task's preparation is done, all task attributes must be consumed.

Dependencies are represented by links. A link is created by

``` python
ctx.link(linkname, *files)
```

The files can be again filenames or 2-tuples and define which files from the dependency will by symlinked to the dependant's directory. If the dependency blocks parent's preparation, the link can be created with keyword argument `needed=True`. The actual dependency between tasks is created with

``` python
task1 + link + task2
```

which declares `task1` as a dependency of `task2` and returns `task2`. Using this API, tasks can be easily chained with

``` python
ctx(...) + ctx.link(...) + ctx(...) + ctx.link(...) + ...
```

A task can have multiple dependencies, defined by simply adding to multiple links. This can be done in two ways,

``` python
links = [ctx(...) + ctx.link(...) for ... in ...]
# now either 
for link in links:
	link + task
# or more simply
links + task
```

If multiple parents depend on a single child, this has to be done explicitly with

``` python
links = [ctx.link(...) + ctx(...) for ... in ...]
for link in links:
	task + link
```

Builds in Caf are simply collections of tasks. Builds are organized in targets. A task is added to a target with

``` python
task + ctx.target(targetname, linkname)
```

If a target contains a single task, `linkname` can be omitted and the target will be directly a symlink, otherwise the target will be a directory containing symlinks with names given by `linkname`.