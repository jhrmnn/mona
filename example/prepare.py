#!/usr/bin/env python
import dispatcher
import pyexample

tasks = [[('base', (b, b))] for b in range(5)]
dispatcher.dispatch('RUN', tasks, pyexample.prepare)
