#!/usr/bin/env python
from collections import defaultdict
from functools import wraps


class Parser(object):
    def __init__(self):
        self.parsers = {}

    def readline(self):
        self.line = self.f.readline()
        if not self.line:
            raise EOFError('Unexpected end of file')
        return self.line

    def parse(self, f):
        self.results = defaultdict(dict)
        self.f = f
        while True:
            try:
                self.readline()
            except EOFError:
                break
            for key in self.parsers:
                if key in self.line:
                    self.parsers[key](self, key)
                    break
        return dict(self.results)

    def add(self, key):
        def decorator(f):
            @wraps(f)
            def wrapper(self, key):
                return_code = f(self)
                if return_code:
                    del self.parsers[key]
            self.parsers[key] = wrapper
            return wrapper
        return decorator
