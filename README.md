# `caf` — Calculation framework

Caf is a general distributed build system written in Python 3.6, with focus on reproducible scientific calculations, directly inspired by [Waf](https://waf.io) and vaguely by Git.

## Architecture

In Caf, each build task represents a single shell command and a directory of inputs files (relative symbolic links, directories), which together are hashed with SHA1 that uniquely identifies a given task. The input files cannot be modified by the shell command, while all files created by the task command are recorded as output files. The command should have no side effects besides a current working directory. An output file of one task can become an input file of another task, which creates task dependencies. For the purpose of task hashing, the SHA1 hash of such an input file is determined by the hash of its generating parent task and its name, not by its content. So whereas a task must have exactly the same shell command and direct input files on different machines to be considered the same task, the contents of its input files coming from other tasks may be different. This reflects the reality that most programs do not produce bitwise deterministic output, but only semantically deterministic output.

A particular Caf build consists of a mapping from task labels, in the form of Posix paths, to task hashes. A single task can have multiple labels. This leads to a natural representation of the nameless task dependency tree as a directory tree.

## Using Caf

A particular Caf build is defined in a calculation script (cscript), `cscript.py`, which is to Caf what a makefile is to Make. This section documents the use of Caf with an already written cscript, while the next one documents how cscripts are written.

Caf is controlled by commands, `./caf <command>`, the list of which can be obtained by running `./caf`. The standard approach of configuring a build and then executing it, most often done with the omnipresent `./configure` and `make`, corresponds to `./caf conf` and `./caf make`. Running the configuration step for the first time creates the `.caf` directory, which holds all data related to Caf. Two sqlite database files are created, `.caf/index.db` keeps information about all tasks ever configured and `.caf/queue.db` tracks which tasks are to be executed in the make step. `.caf/objects` is either a directory or a link to a directory that keeps contents of all files from all tasks, hashed with SHA1. `.caf/log` keeps a timestamped log of all Caf commands executed. The configuration step then loads the cscript, executes it, creates the make tasks defined in it, stores the task metadata in `.caf/index.db`, the file contents in `.caf/objects`, and schedules the task execution to `.caf/queue.db`. The make step then reads the tasks from `.caf/queue.db`, fetches them from `.caf/index.db` and `.caf/objects`, executes them, and stores the generated files back to `.caf/index.db` and `.caf/objects`.



## Configuring Caf

The behavior of Caf is influenced by two configuration files, a global in `~/.config/caf/config.ini` and a local (with higher preference) in `.caf/config.ini`. The syntax is that of the `configparser` module in Python standard library. The possible configuration options, listed as `<section>/<key>`, are:

-   `core/cache`: Where file storages of all task files are created.

## Writing cscripts

// TODO
