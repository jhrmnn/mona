# `caf` — Calculation framework

**[prototype]**

Caf is a distributed build system written in Python 3 that draws on the idea that generated content (scientific calculation, program build) is input/dependency-addressable in the same way that static content is content-addressable and then applies design similar to [Git](https://git-scm.com) – each build task lives in a directory that is hashed by the task's inputs/dependencies and stored by its hash address. Unlike traditional build systems, in which the dependency tree is defined in a build script and lives only in memory during runtime of the build system, Caf's dependency tree is defined by the symlink tree within the hash-addressed task database, and the build script serves merely to navigate within this tree. The API of Caf is heavily inspired by the [Waf](https://waf.io) build system. Some names of objects were taken from [Homebrew](http://brew.sh).

The main executable `caf` is supposed to live in the project's root directory, together with the `caflib` directory. If used in a git-versioned project, the `.gitignore` file from this repository might be useful. In Caf, the project's build flow is defined in a Python source file named `cscript`, examples of which can be found in directory `examples`.

There are two main parts to the Caf file environment, the Caf repository and the build directory. The repository lives in directory `_caf` and contains two directories: tasks are initially prepared in `Brewery` and once their hash address is known, they are moved to `Cellar`. Builds in Caf are merely "views" (symlinks) into the repository of tasks and live in directory `build`. There is always only a single Cellar, but there may be many batches in `Brewery` and many builds, which are named by a timestamp. The latest batch and build are always symlinked to `Latest`.

Each build task lives in its own directory and is defined by its contents, addresses of other tasks that might be its dependencies and file `command` that contains a shell command that executes the tasks.

## Cscript

The project's build flow is defined in file `cscript`. A minimal script, which does nothing, contains

```python
def build(ctx):
    pass
```

The single argument `ctx` of the `build` function is a so-called build context that gives access to the API of Caf. Tasks are defined by calling the build context with keyword arguments (task attributes), the only mandatory attribute being `command`. A minimal task, which does nothing, is defined by

```python
ctx(command='')
```

Other notable task attributes that might be present are:

- *files*: A list of filenames or a 2-tuples of filenames, that are to be copied from project's directory to the task directory. If a tuple `t` is given, file `t[0]` in the project directory is copied to file `t[1]` in the task directory.
- *templates*: Similar to `files`, but the file is first processed and all instances of `{{ <task attribute> }}` are replaced.
- *features*: A list of functions or names of registered functions that are executed in the task directory after *files* and *templates* are applied. Features accept a single argument, the task object, and access to task attributes is given by `task.consume(attribute)`.

For all three attributes above, passing `[x]` is equivalent to passing simply `x`. Any other user attributes might be passed to `ctx`, and those can then be consumed by various `features`. An important rule is that after the task's preparation is done, all task attributes must be consumed.

The core ingredient of any build system is the dependency between tasks. In Caf, these are represented by links between tasks (parents) and their dependencies (children). A link is created by calling

```python
ctx.link(linkname, file1, file2,...)
```

The files can be again filenames or 2-tuples and define which files from the child will by symlinked to the parent's directory. If a generated file from the dependency is used by a parent's feature, the link can be created with keyword argument `needed=True`, which informs Caf that the preparation of the parent cannot be attempted before the child has been executed.

The actually dependency between tasks is created with

```python
task1 + link + task2
```
which declares `task1` as a dependency of `task2` and returns `task2`. Using this API, tasks can be easily chained with

```python
ctx(...) + ctx.link(...) + ctx(...) + ctx.link(...) + ...
```

A task can have multiple dependencies, defined by simply adding to multiple links. This can be done in two ways,

```python
links = [ctx(...) + ctx.link(...) for ... in ...]
# now either 
for link in links:
	link + task
# or more simply
links + task
```

If multiple parents depend on a single child, this has to be done explicitly with

```python
links = [ctx.link(...) + ctx(...) for ...  in ...]
for link in links:
	task + link
```

Builds in Caf are simply views of tasks. Builds are organized in targets. A task is added to a target with

```python
task + ctx.target(targetname, linkname)
```

If a target contains a single task, `linkname` can be omitted and the target will be directly a symlink, otherwise the target will be a directory containing symlinks with names given by `linkname`.
