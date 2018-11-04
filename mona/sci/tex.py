# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import Dict

from jinja2 import Template


def jinja_tex(tex_template: str, ctx: Dict[str, object]) -> str:
    jinja_template = Template(
        tex_template,
        variable_start_string=r'<<',
        variable_end_string='>>',
        block_start_string='<+',
        block_end_string='+>',
        comment_start_string='<#',
        comment_end_string='#>',
        trim_blocks=True,
        autoescape=False,
    )
    return jinja_template.render(ctx)
