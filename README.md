# `caf` — Calculation framework

Caf is a distributed build system, inspired by [Waf](https://waf.io), [Git](https://git-scm.com) and [Homebrew](http://brew.sh) and written in Python 3. It is based on a build model in which

- each build task lives in its own directory
- tasks can have other tasks as dependencies and these are recorded as symlinks
- each task goes through three stages: creation, preparation and execution
- both preparation and execution of a task can be blocked by unexecuted dependencies, but creation is never blocked; that is, the dependency tree is static
- when the preparation of a task is unblocked, it is prepared, hashed by the contents of its directory and stored by the hash
- tasks are organised in targets, which are simply collections of tasks
- execution of tasks is performed by independent workers which communicate only via the file system

The above model is implemented in an offline manner, which means that creation and preparation are handled with `caf build`, the execution is handled with `caf work` and these two need to be repeated as many times as needed for all tasks to be completed. In scenarios that are typically handled by build systems, however, preparation of tasks does not depend on its dependencies (only the execution) and hence a single run of `caf build work` is needed to prepare and execute all tasks.

Caf creates files in two directories.

1. `_caf` is the repository of tasks. It contains `Brewery` with multiple batches of created but not prepared tasks and a single `Cellar` with prepared and hashed tasks.
2. `build` is the repository of targets. It contains batches of targets which are collections of symlinks to tasks in `Brewery` or `Cellar`.

## Cscript

The equivalent of a Makefile in Caf is `cscript`. A minimal Cscript, which does nothing, contains

``` python
def build(ctx):
    pass
```

The single argument `ctx` of the `build` function is a so-called build context which gives access to the API of Caf. Tasks are defined by calling the build context with keyword arguments (task attributes), the only mandatory attribute being `command`. A minimal task, which does nothing, is defined by

``` python
ctx(command='')
```

Other notable task attributes that might be present are:

- *files*: A list of filenames or 2-tuples of filenames, that are to be copied from project's directory to the task directory. If a tuple `t` is given, file `t[0]` in the project directory is copied to file `t[1]` in the task directory.
- *templates*: Similar to `files`, but the file is first processed and all instances of `{{ <task attribute> }}` are replaced.
- *features*: A list of functions or names of registered functions that are executed in the task directory after *files* and *templates* are applied. Features accept a single argument, the task object, and access to task attributes is given by `task.consume(attribute)`.

For all three attributes above, passing `[x]` is equivalent to passing `x`. Any other user attributes might be passed to `ctx`, and those can then be consumed by various `features`. An important rule is that after the task's preparation is done, all task attributes must be consumed.

Dependencies are represented by links. A link is created by

``` python
ctx.link(linkname, file1, file2,...)
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

Builds in Caf are simply views of tasks. Builds are organized in targets. A task is added to a target with

``` python
task + ctx.target(targetname, linkname)
```

If a target contains a single task, `linkname` can be omitted and the target will be directly a symlink, otherwise the target will be a directory containing symlinks with names given by `linkname`.