#!/usr/bin/env python3
import datetime
import toml
import sys
import os

sys.path.insert(0, os.path.abspath('..'))

metadata = toml.load(open('../pyproject.toml'))['tool']['poetry']

project = metadata['name']
version = metadata['version']
author = metadata['authors'][0]

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx.ext.viewcode'
]
source_suffix = '.rst'
master_doc = 'index'
copyright = f'2016-{datetime.date.today().year}, {author}'
release = version
language = None
exclude_patterns = ['build', '.DS_Store']
pygments_style = 'sphinx'
todo_include_todos = True
html_theme = 'alabaster'
html_theme_options = {
    'description': 'Distributed',
    'github_button': True,
    'github_user': 'azag0',
    'github_repo': 'pyberny',
}
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'relations.html',
        'searchbox.html',
    ]
}
htmlhelp_basename = f'{project}doc'


def skip_namedtuples(app, what, name, obj, skip, options):
    if hasattr(obj, '_source'):
        return True


def setup(app):
    app.connect('autodoc-skip-member', skip_namedtuples)
