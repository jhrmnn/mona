# `caf` — Calculation framework

**[prototype, in development, unstable]**

Caf helps to maintain this general problem:

1. Create a set of atomic calculation tasks.
1. Consume tasks with a set of workers, either locally or remotely.
1. Extract data from finished tasks, if remote, fetch.
1. Process data to obtain final results.

Caf is built around the idea that running a calculation is not very different from compiling: it transforms inputs (sources) to results (binaries). (Although not a primary target, one can use Caf to compile variants of programs, see example below.) Caf draws inspiration heavily from the excellent build framework [Waf](http://waf.io). Caf tries to be for running calculations what Make is for compiling: it knows how to get from inputs to results and it caches in-between steps.  

The motivation for having a system like Caf is the same as for Make: automatization and reproducibility. It is especially important in science to keep track of how calculations are done. Caf itself provides automatization and reproducibility is achieved by wrapping Git: each Caf project is a clone of this repository and results are tied to a commit.

### Prerequisites

	pip install pyyaml docopt pathlib

Some optional tools such as `geomlib` also require NumPy.

### How to

You can try Caf by cloning this repository, switching to branch `example-basic`

	git checkout example-basic

and running

	./caf run
	./caf process

To see what happened, have a look in the generated `_cache` and `build` directories. To see why it happened, have a look at files [`a.in`](https://github.com/azag0/caf/blob/example-basic/a.in), [`b.in`](https://github.com/azag0/caf/blob/example-basic/b.in) and [`cscript`](https://github.com/azag0/caf/blob/example-basic/cscript).

An example illustrating a use of Caf for building variants of programs is prepared in branch `example-compile` (requires `gcc`)

	git checkout example-compile
	./caf run
	./caf process

and the relevant files are [`hello.c`](https://github.com/azag0/caf/blob/example-compile/hello.c) and [`cscript`](https://github.com/azag0/caf/blob/example-compile/cscript).
