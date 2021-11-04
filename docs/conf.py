import datetime
import inspect
import os
import sys
import warnings
from unittest.mock import MagicMock

import toml
from sphinx.util.inspect import Signature  # type: ignore


def signature_init(self, *args, **kwargs):
    _signature_init(self, *args, **kwargs)
    self.annotations.clear()
    params = [
        param.replace(annotation=inspect.Parameter.empty)
        for param in self.signature.parameters.values()
    ]
    self.signature = self.signature.replace(
        parameters=params, return_annotation=inspect.Signature.empty
    )


Signature.__init__, _signature_init = signature_init, Signature.__init__


class Mock(MagicMock):
    @classmethod
    def __getattr__(cls, name):
        return MagicMock()


MOCK_MODULES = [
    'textx',
    'textx.metamodel',
    'numpy',
    # for python 3.6 (readthedocs)
    'contextvars',
    'contextlib',
    'importlib.resources',
]
sys.modules.update((mod_name, Mock()) for mod_name in MOCK_MODULES)
sys.path.insert(0, os.path.abspath('..'))

warnings.filterwarnings('ignore', r'formatargspec\(\) is now deprecated.')

with open('../pyproject.toml') as f:
    metadata = toml.load(f)['tool']['poetry']

project = 'Mona'
version = metadata['version']
author = ' '.join(metadata['authors'][0].split()[:-1])
description = metadata['description']

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.mathjax',
]
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'graphviz': ('https://graphviz.readthedocs.io/en/stable', None),
}
source_suffix = '.rst'
master_doc = 'index'
copyright = f'2015-{datetime.date.today().year}, {author}'
release = version
language = None
exclude_patterns = ['build', '.DS_Store']
pygments_style = 'sphinx'
todo_include_todos = True
html_theme = 'alabaster'
html_theme_options = {
    'description': description,
    'github_button': True,
    'github_user': 'jhrmnn',
    'github_repo': 'mona',
    'badge_branch': 'master',
    'codecov_button': True,
    'travis_button': True,
}
html_sidebars = {
    '**': ['about.html', 'navigation.html', 'relations.html', 'searchbox.html']
}
htmlhelp_basename = f'{project}doc'
autodoc_default_options = {'special-members': '__call__'}
