#!/usr/bin/env python3

import sys
import os
from recommonmark.parser import CommonMarkParser

sys.path.insert(0, os.path.abspath('..'))

extensions = ['sphinx.ext.autodoc']
templates_path = ['_templates']
source_suffix = ['.rst', '.md']
source_parsers = {'.md': CommonMarkParser}
master_doc = 'index'
project = 'Caf'
author = 'Jan Hermann'
version = '0.3'
release = '0.3.0'
language = None
exclude_patterns = ['_build']
pygments_style = 'sphinx'
todo_include_todos = False
modindex_common_prefix = ['caflib']
html_theme = 'alabaster'
html_static_path = ['_static']
html_show_copyright = False
htmlhelp_basename = 'Cafdoc'
