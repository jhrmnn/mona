# -*- coding: utf-8 -*-
from distutils.core import setup

packages = \
['mona', 'mona.plugins', 'mona.sci', 'mona.sci.aims']

package_data = \
{'': ['*']}

install_requires = \
['click>=7.0,<8.0',
 'graphviz>=0.10.0,<0.11.0',
 'toml>=0.9.6,<0.10.0',
 'typing_extensions>=3.6,<4.0']

extras_require = \
{'cov': ['coverage>=4.5,<5.0'],
 'doc': ['sphinx>=1.8,<2.0', 'sphinxcontrib-asyncio>=0.2.0,<0.3.0'],
 'sci': ['numpy>=1.15,<2.0', 'textx>=1.5,<1.6', 'jinja2>=2.10,<3.0'],
 'test': ['pytest>=3.8,<4.0', 'pytest-mock>=1.10,<2.0']}

entry_points = \
{'console_scripts': ['mona = mona.cli:cli']}

setup_kwargs = {
    'name': 'mona',
    'version': '0.2.6',
    'description': 'Calculation framework',
    'long_description': '# Mona\n\n[![build](https://img.shields.io/travis/jhrmnn/mona/master.svg)](https://travis-ci.org/jhrmnn/mona)\n[![coverage](https://img.shields.io/codecov/c/github/jhrmnn/mona.svg)](https://codecov.io/gh/jhrmnn/mona)\n![python](https://img.shields.io/pypi/pyversions/mona.svg)\n[![pypi](https://img.shields.io/pypi/v/mona.svg)](https://pypi.org/project/mona/)\n[![commits since](https://img.shields.io/github/commits-since/jhrmnn/mona/latest.svg)](https://github.com/jhrmnn/mona/releases)\n[![last commit](https://img.shields.io/github/last-commit/jhrmnn/mona.svg)](https://github.com/jhrmnn/mona/commits/master)\n[![license](https://img.shields.io/github/license/jhrmnn/mona.svg)](https://github.com/jhrmnn/mona/blob/master/LICENSE)\n[![code style](https://img.shields.io/badge/code%20style-black-202020.svg)](https://github.com/ambv/black)\n\nMona is a calculation framework that provides [persistent](https://en.wikipedia.org/wiki/Persistence_(computer_science)) [memoization](https://en.wikipedia.org/wiki/Memoization) and turns the Python call stack into a task [dependency graph](https://en.wikipedia.org/wiki/Dependency_graph). The graph contains three types of edges: a task input depending on outputs of other tasks, a task creating new tasks, and a task output referencing outputs of other tasks.\n\n## Installing\n\nInstall and update using [Pip](https://pip.pypa.io/en/stable/quickstart/).\n\n```\npip install -U mona\n```\n\n## A simple example\n\n```python\nfrom mona import Mona, Rule\n\napp = Mona()\n\n@Rule\nasync def total(xs):\n    return sum(xs)\n\n@app.entry(\'fib\', int)\n@Rule\nasync def fib(n):\n    if n <= 2:\n        return 1\n    return total([fib(n - 1), fib(n - 2)])\n```\n\n```\n$ export MONA_APP=fib:app\n$ mona init\nInitializing an empty repository in /home/mona/fib/.mona.\n$ mona run fib 5\n7c3947: fib(5): will run\n0383f6: fib(3): will run\nb0287d: fib(4): will run\nf47d51: fib(1): will run\n9fd61c: fib(2): will run\n45c92d: total([fib(2), fib(1)]): will run\n2c136c: total([fib(3), fib(2)]): will run\n521a8b: total([fib(4), fib(3)]): will run\nFinished\n$ mona graph\n```\n\n<img src="https://raw.githubusercontent.com/jhrmnn/mona/master/docs/fib.svg?sanitize=true" alt width="350">\n\n```python\nfrom fib import app, fib\n\nwith app.create_session() as sess:\n    assert sess.eval(fib(5)) == sum(sess.eval([fib(4), fib(3)]))\n```\n\n## Links\n\n- Documentation: https://jhrmnn.github.io/mona\n\n',
    'author': 'Jan Hermann',
    'author_email': 'dev@janhermann.cz',
    'url': 'https://github.com/jhrmnn/mona',
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'extras_require': extras_require,
    'entry_points': entry_points,
    'python_requires': '>=3.7,<4.0',
}


setup(**setup_kwargs)
