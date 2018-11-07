# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import io
import xml.etree.ElementTree as ET
from typing import IO, Any, Dict, Type

import numpy as np  # type: ignore

from ...files import File
from ...rules import Rule

__version__ = '0.1.0'
__all__ = ['parse_aims']


@Rule
async def parse_aims(outputs: Dict[str, File]) -> Any:
    """Create a task that parses outputs of FHI-aims calculations.

    The task takes the output of :class:`mona.sci.aims.Aims` as an input and
    returns a dictionary of parsed results as output.
    """
    stdout = outputs['results.xml'].read_text()
    parsed = parse_xml(io.StringIO(stdout))
    energies = {x['name']: x['value'][0] for x in parsed['energy']}
    return {'energy': energies['Total energy']}


def parse_xml(source: IO[str]) -> Any:
    root = ET.parse(source).getroot()
    return parse_xmlelem(root)


def parse_xmlelem(elem: Any) -> Any:
    results = {}
    children = {c.tag for c in elem}
    for child in children:
        child_elems = elem.findall(child)
        child_results = []
        for child_elem in child_elems:
            if 'type' in child_elem.attrib:
                if 'size' in child_elem.attrib:
                    child_elem_results = parse_xmlarr(child_elem)
                else:
                    child_elem_results = float(child_elem.text)
            elif len(list(child_elem)):
                child_elem_results = parse_xmlelem(child_elem)
            else:
                child_elem_results = child_elem.text.strip()
            child_results.append(child_elem_results)
        if len(child_results) == 1:
            results[child] = child_results[0]
        else:
            results[child] = child_results
    return results


def parse_xmlarr(xmlarr: Any, axis: int = None, typef: Type[Any] = None) -> Any:
    if axis is None:
        axis = len(xmlarr.attrib['size'].split()) - 1
    if not typef:
        typename = xmlarr.attrib['type']
        if typename == 'dble' or typename == 'real':
            typef = float
        elif typename == 'int':
            typef = int
        else:
            raise Exception('Unknown array type')
    if axis > 0:
        lst = [
            parse_xmlarr(v, axis - 1, typef)[..., None]
            for v in xmlarr.findall('vector')
        ]
        return np.concatenate(lst, axis)
    else:
        return np.array([typef(x) for x in xmlarr.text.split()])
