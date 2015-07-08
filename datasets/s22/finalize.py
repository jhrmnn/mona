#!/usr/bin/env python
import re
import sys
from pathlib import Path
import geomlib
import json
import csv
from difflib import SequenceMatcher


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


geoms = []
for path in sys.argv[1:]:
    path = Path(path)
    geom = geomlib.readfile(path)
    code, label = re.findall(r'(\d+)_(\w+)', path.stem)[0]
    code = int(code)
    frags = geom.getfragments()
    try:
        assert len(frags) == 2 and geomlib.concat(frags) == geom
    except AssertionError:
        sys.stderr.write('Error: {} ({}) was not fragmented correctly\n'
                         .format(label, code))
    geoms.append({'label': label,
                  'code': code,
                  'complex': geom,
                  'fragments': frags})

geoms.sort(key=lambda x: x['code'])
geomlbls = [g['label'] for g in geoms]
energies = json.load(sys.stdin)
enelbls = [row['system name'] for row in energies]
energies = [energies[l.index(max(l))] for l in
            [[similar(a, b) for a in enelbls] for b in geomlbls]]

writer = csv.DictWriter(sys.stdout, fieldnames=energies[0].keys())
writer.writeheader()
writer.writerows(energies)
for idx, row in enumerate(geoms):
    row['complex'].write('{}-complex.xyz'.format(idx+1))
    for i in range(2):
        row['fragments'][i].write('{}-monomer-{}.xyz'.format(idx+1, i+1))
