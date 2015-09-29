# `caf` — Calculation framework

**[prototype]**

Caf is a distributed build system that draws on the idea that reproducibly generated content (software build, scientific calculation) is dependency-addressable in the same way that static content is content-addressable and then applies design similar to Git – each build task lives in a directory that is hashed by the task's dependencies and stored by its hash address. Unlike traditional build systems, in which the dependency tree is defined in a build script and lives only in memory during runtime of the build system, Caf's dependency tree is defined by the symlink tree within the dependency-addressed task database, and the build script serves merely to navigate within this tree.
