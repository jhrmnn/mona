# `caf` — Calculation frameworpk

**[proposal]**

Caf is a distributed build system that draws on the idea that reproducibly generated content (software build, scientific calculation) is dependency-addressable in the same way that static content is content-addressable and then applies the same design as Git – each build task lives in a directory that is hashed by the task's dependencies and stored by its hash address. Unlike traditional build trees that are defined in a "makefile", but the dependency tree lives only in memory during runtime of the build system, Caf's dependency tree is defined by the symlink tree within the dependency-addressed task database, and the "makefile" serves only to navigate within this tree.
