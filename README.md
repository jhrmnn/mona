# `caf` – Calculation framework

**[prototype, in development, unstable]**

`caf` helps to maintain this general problem:

1. Create a set of calculation tasks with possible dependencies.
1. Consume tasks with a set of workers, either locally or remotely, satisfying said dependencies.
1. Extract data from finished tasks, and if remote, fetch them locally.
1. Process data to obtain final results.

`caf` is built around the idea that a calculation is not very different from a compilation: it transforms inputs (sources) to results (binaries). `caf` tries to be for a calculation what Make is for a compilation: it knows how to get from inputs to results and it maintains dependencies. `caf` can do recursion: inputs of a `caf` calculation can depend on a child `caf` calculation. This enables building complex layered calculations.

The motivation for having a system like `caf` is the same as for Make: automatization and reproducibility. It is especially important in science to keep track of how calculations are done. Apart from using the philosophy of Make, `caf` achieves this goal by wrapping git: every `caf` calculation is a clone of this repository and every child calculation is a git submodule.
