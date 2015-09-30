from collections import defaultdict
from functools import wraps


class Parser(object):
    def __init__(self, hook=None):
        self.parsers = {}
        self.hook = hook

    def readline(self, hook=None):
        self.line = self.f.readline()
        if not self.line:
            raise EOFError('Unexpected end of file')
        hook = hook or self.hook  # local hook or global hook
        if hook:
            self.line = hook(self.line)  # apply hook
        return self.line

    def parse(self, f):
        self.results = defaultdict(dict)
        self.f = f
        while True:
            try:
                self.readline(lambda s: s)  # read line without hook
            except EOFError:
                break
            for key in self.parsers:
                if key in self.line:
                    if self.hook:  # apply global hook to already read line
                        self.line = self.hook(self.line)
                    self.parsers[key](self, key)  # dispatch to parser
                    break
        return dict(self.results)

    def add(self, key):
        def decorator(f):
            @wraps(f)
            def wrapper(self, key):
                f(self)
            self.parsers[key] = wrapper
            return wrapper
        return decorator
