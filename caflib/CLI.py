from docopt import docopt
from textwrap import dedent
from collections import OrderedDict
from caflib.Logging import Table


class Executor:
    def __init__(self, func, mapping=None, doc=None):
        self.func = func
        self.mapping = mapping or func.__annotations__
        self._doc = doc or func.__doc__

    def __str__(self):
        s = dedent(self._doc).rstrip()
        firstline, s = s.split('\n', 1)
        if firstline:
            s = firstline + '\n' + s
        return s

    def __format__(self, fmt):
        if not fmt:
            return str(self)
        elif fmt == 'header':
            return str(self).split('\n', 1)[0]
        else:
            raise ValueError('Invalid format specifier')

    def parse(self, argv):
        return docopt(str(self), argv=argv[1:])

    def __call__(self, argv, ctx=None):
        args = self.parse(argv)
        kwargs = {}
        for key, name in self.mapping.items():
            if isinstance(name, tuple):
                name, func = name
                if args[name] is not None:
                    if isinstance(func, str):
                        if ctx:
                            kwargs[key] = getattr(ctx, func)(args[name])
                        else:
                            raise RuntimeError('Executor "{}" has no context for "{}"'
                                               .format(self.func.__name__, func))
                    else:
                        kwargs[key] = func(args[name])
                else:
                    kwargs[key] = None
            else:
                kwargs[key] = args[name]
        if ctx:
            return self.func(ctx, **kwargs)
        else:
            return self.func(**kwargs)


class CLIExit(SystemExit):
    pass


class CLIMeta(type):
    def __new__(cls, name, bases, namespace):
        cls = type.__new__(cls, name, bases, namespace)
        if cls.__base__ == object:
            cls.commands = OrderedDict()
        else:
            cls.commands = cls.__base__.commands.copy()
        return cls


class CLI(metaclass=CLIMeta):

    def __init__(self, name):
        self.name = name
        self.commands = self.__class__.commands.copy()

    def __str__(self):
        return '{self:usage}\n\n{self:commands}'.format(self=self)

    def __format__(self, fmt):
        if not fmt:
            return str(self)
        elif fmt == 'usage':
            usage = """\
            Usage:
                {} COMMAND [ARGS...]
            """.format(self.name).rstrip()
            return dedent(usage)
        elif fmt == 'commands':
            table = Table(align='<', indent='    ', sep='    ')
            for trigger, func in self.commands.items():
                if len(trigger) == 1:
                    table.add_row(trigger[0], format(func, 'header'))
            return 'Commands:\n{}'.format(table)
        elif fmt == 'help':
            parts = []
            for part in ['header', 'usage', 'options', 'commands']:
                try:
                    parts.append(format(self, part))
                except ValueError:
                    pass
            return '\n\n'.join(parts)
        else:
            raise ValueError('Invalid format specifier')

    def find_command(self, argv):
        nargv = len(argv)-1
        maxlen = min(max(map(len, self.commands.keys())), nargv)
        trigger = None
        for tlen in range(maxlen, 0, -1):
            for trigger in self.commands:
                if trigger == tuple(argv[1:1+tlen]):
                    break
            else:
                trigger = None
            if trigger:
                break
        return trigger

    def parse(self, argv):
        trigger = self.find_command(argv)
        if not trigger:
            self.exit()
        return self.commands[trigger].parse(argv)

    def __call__(self, argv):
        trigger = self.find_command(argv)
        if not trigger:
            self.exit()
        return self.commands[trigger](argv, ctx=self)

    def exit(self):
        raise CLIExit(str(self))

    @classmethod
    def command(cls, triggers=None):
        def decorator(func):
            executor = Executor(func)
            cls.commands[(func.__name__.strip('_'),)] = executor
            for trigger in triggers or []:
                cls.commands[tuple(trigger.split())] = executor
            return executor
        return decorator


@CLI.command()
def help(cli, command: 'COMMAND'):
    """
    Print help for individual commands.

    Usage:
        {program} help [COMMAND]
    """
    if command:
        trigger = (command,)
        if trigger in cli.commands:
            print(cli.commands[trigger])
        else:
            cli.exit()
    else:
        print(format(cli, 'help'))
