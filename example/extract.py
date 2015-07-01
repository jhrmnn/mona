#!/usr/bin/env python
import dispatcher


def extractor(path):
    return int((path/'run.log').open().read())

dispatcher.extract('RUN', extractor)
