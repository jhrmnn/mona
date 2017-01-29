from textwrap import dedent
from collections import OrderedDict
import sys

try:
    from docopt import docopt
except ImportError:
    print('Error: Cannot import docopt')
    sys.exit(1)


class Command:
    def __init__(self, func, name=None, mapping=None, doc=None):
        self.name = name or func.__name__
        self._func = func
        self._mapping = mapping or func.__annotations__
        self._doc = doc or func.__doc__

    def __repr__(self):
        return f'<Command "{self.name}">'

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

    def parse(self, argv, *clis):
        command = ' '.join(cli.name for cli in clis) + ' ' + self.name
        doc = str(self).replace('<command>', command)
        return docopt(doc, argv=argv[1:])

    def __call__(self, argv, *clis):
        args = self.parse(argv, *clis)
        kwargs = {}
        for key, name in self._mapping.items():
            if isinstance(name, tuple):
                name, func = name
                if args[name] is not None:
                    if isinstance(func, str):
                        for ctx in clis:
                            if hasattr(ctx, func):
                                kwargs[key] = getattr(ctx, func)(args[name])
                                break
                        else:
                            raise RuntimeError(
                                f'{self!r} has no context for "{func}"'
                            )
                    else:
                        kwargs[key] = func(args[name])
                else:
                    kwargs[key] = None
            else:
                kwargs[key] = args[name]
        return self._func(*clis, **kwargs)


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


def bind_function(obj, func, name=None, triggers=None):
    command = Command(func, name=name)
    obj.commands[(command.name.strip('_'),)] = command
    for trigger in triggers or []:
        obj.commands[tuple(trigger.split())] = command
    return command


class CLI(metaclass=CLIMeta):

    def __init__(self, name, header=None):
        self.name = name
        self.header = header
        self.commands = self.__class__.commands.copy()

    def __repr__(self):
        return f'<CLI "{self.name}">'

    def __str__(self):
        parts = []
        for part in ['header', 'usage', 'options', 'commands']:
            try:
                parts.append(format(self, part))
            except ValueError:
                pass
        return '\n\n'.join(parts)

    def __format__(self, fmt):
        if not fmt:
            return str(self)
        elif fmt == 'header':
            if self.header:
                return self.header
            else:
                raise ValueError('Invalid format specifier')
        elif fmt == 'usage':
            usage = """\
            Usage:
                <command> COMMAND [ARGS...]
            """.rstrip()
            return dedent(usage)
        elif fmt == 'commands':
            maxwidth = max(len(trigger) for trigger, *_ in self.commands)
            commands = [
                f'    {trigger:{maxwidth}}    {command:header}'
                for (trigger, *other), command in self.commands.items()
                if not other
            ]
            return 'Commands:\n{}'.format('\n'.join(commands))
        elif fmt == 'short':
            return f'{self:usage}\n\n{self:commands}'
        else:
            raise ValueError('Invalid format specifier')

    def find_command(self, argv, *clis):
        offset = len(clis)+1
        nargv = len(argv)-offset
        maxlen = min(max(map(len, self.commands.keys())), nargv)
        trigger = None
        for tlen in range(maxlen, 0, -1):
            for trigger in self.commands:
                if trigger == tuple(argv[offset:offset+tlen]):
                    break
            else:
                trigger = None
            if trigger:
                break
        return trigger

    def parse(self, argv, *clis):
        trigger = self.find_command(argv, *clis)
        if not trigger:
            self.exit(clis)
        clis += (self,)
        return self.commands[trigger].parse(argv, *clis)

    def __call__(self, argv, *clis):
        trigger = self.find_command(argv, *clis)
        if not trigger:
            self.exit(clis)
        clis += (self,)
        return self.commands[trigger](argv, *clis)

    def exit(self, clis):
        clinames = ' '.join(cli.name for cli in clis + (self,))
        raise CLIExit(format(self, 'short').replace('<command>', clinames))

    def add_command(self, **kwargs):
        def decorator(func):
            return bind_function(self, func, **kwargs)
        return decorator

    @classmethod
    def command(cls, **kwargs):
        def decorator(func):
            return bind_function(cls, func, **kwargs)
        return decorator


@CLI.command()
def help(*clis, command: 'COMMAND'):
    """
    Print help for individual commands.

    Usage:
        <command> [COMMAND]
    """
    cli = clis[-1]
    clinames = ' '.join(cli.name for cli in clis)
    if command:
        trigger = (command,)
        if trigger in cli.commands:
            command = cli.commands[trigger]
            print(str(command).replace('<command>', clinames + ' ' + command.name))
        else:
            cli.exit()
    else:
        print(str(cli).replace('<command>', clinames))
