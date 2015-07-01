#!/usr/bin/env python
from pathlib import Path
import shutil
import geomlib
import os
from logparser import Parser
import re
import xml.etree.cElementTree as ET
import numpy as np


def prepare(path, task):
    path = Path(path)
    path.mkdir(parents=True)
    if 'geom' in task:
        geom = task['geom']
    elif Path('geometry.in').is_file():
        geom = geomlib.readfile('geometry.in', 'fhiaims')
    geom.write(path/'geometry.in', 'fhiaims')
    species = set((a.number, a.symbol) for a in geom.atoms)
    with Path('control.in').open() as f:
        template = f.read()
    with Path('basis').open() as f:
        basis = f.read().strip()
    with Path('aims').open() as f:
        aims = f.read().strip()
    basisroot = Path(os.environ['AIMSROOT'])/basis
    with (path/'control.in').open('w') as f:
        f.write(template % task)
        for specie in species:
            f.write(u'\n')
            with (basisroot/('%02i_%s_default' % specie)).open() as fspecie:
                f.write(fspecie.read())
    try:
        aimsbin = next(Path(os.environ['AIMSROOT']).glob(aims))
    except StopIteration:
        raise Exception('Cannot find binary %s' % aims)
    Path(path/'aims').symlink_to(aimsbin)
    shutil.copy('run_aims.sh', str(path/'run'))
    os.system('chmod +x %s' % (path/'run'))


def parse_aimsxml(path):
    path = Path(path)
    root = ET.parse(str(path)).getroot()
    return parse_xmlelem(root)


def parse_xmlelem(elem):
    results = {}
    children = set(c.tag for c in elem)
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


def parse_xmlarr(xmlarr, axis=None, typef=None):
    if axis is None:
        axis = len(xmlarr.attrib['size'].split())-1
    if not typef:
        typename = xmlarr.attrib['type']
        if typename == 'real':
            typef = float
        elif typename == 'int':
            typef = int
        else:
            raise Exception('Unknown array type')
    if axis > 0:
        lst = [parse_xmlarr(v, axis-1, typef)[..., None]
               for v in xmlarr.findall('vector')]
        return np.concatenate(lst, axis)
    else:
        return np.array(map(typef, xmlarr.text.split()))


aims_parser = Parser()


def scrape_output(path):
    path = Path(path)
    with path.open() as f:
        return aims_parser.parse(f)


@aims_parser.add('The structure contains')
def get_atoms(parser):
    natoms, nelec = \
        re.findall(r'contains\s+(\d+) atoms,  and a '
                   'total of\s+([\d\.]+) electrons.',
                   parser.line)[0]
    parser.results['n_atoms'] = int(natoms)
    parser.results['n_elec'] = float(nelec)
    return True


@aims_parser.add('Self-consistency cycle converged')
def set_converged(parser):
    parser.results['converged'] = True
    return True


@aims_parser.add('Performing Hirshfeld analysis')
def get_hirsh(parser):
    atoms = []
    for i_atom in range(parser.results['n_atoms']):
        atom = {}
        parser.readline()
        atom['element'], = \
            re.search(r'Atom\s+\d+\:\s+(\w+)', parser.readline()).groups()
        for i_line in range(5):
            key, val = \
                re.search(r'\|\s+(.*?)\s*\:(.*)', parser.readline()).groups()
            key = key.strip()
            val = map(float, re.split(r'\s+', val.strip()))
            if len(val) == 1:
                val = val[0]
            atom[key] = val
        for i_line in range(3):
            parser.readline()
        atoms.append(atom)
    parser.results['Hirshfeld'] = atoms
    return True


@aims_parser.add('Many-Body Dispersion')
def get_mbd(parser):
    results = {}
    parser.readline()
    if re.search(r'Dynamic.*polarizability', parser.line):
        parser.readline()
        labels, = re.search(r'\|\s+(\S.*)', parser.readline()).groups()
        labels = re.split(r'\s+', labels.strip())
        val = [[] for label in labels]
        while '---' not in parser.readline():
            r, = re.search(r'\|\s+(\S.*)', parser.line).groups()
            r = map(float, re.split(r'\s+', r.strip()))
            for x, arr in zip(r, val):
                arr.append(x)
        omega = dict(zip(labels, val))
        results['dynamic polarizability'] = omega
        for i_line in range(2):
            parser.readline()
        atoms = []
        for i_atom in range(parser.results['n_atoms']):
            elem, c6, alpha = \
                re.search(r'\|\s+ATOM\s+\d+\s+(\w+)\s+(\S+)\s+(\S+)',
                          parser.readline()).groups()
            atoms.append(dict(elem=elem, c6=float(c6), alpha=float(alpha)))
        results['partitioned C6'] = atoms
        parser.readline()
    parser.results['MBD'] = results
    return True


@aims_parser.add('RPA correlation energy :')
def get_rpa_energy(parser):
    if not parser.results['converged']:
        return False
    while '---' not in parser.readline():
        continue
    while '---' not in parser.readline():
        try:
            key, val = \
                re.search(r'(\w.*?)\s*\:\s+([-\.\d]+)\s+Ha', parser.line).groups()
        except AttributeError:
            continue
        parser.results['energies'][key] = float(val)
    return True


@aims_parser.add('Total energy components')
def get_energy(parser):
    enes = parser.results['energies']
    while '|' in parser.readline():
        try:
            key, val = \
                re.search(r'\|\s+(.*?)\s*\:\s+([-\.\d]+)\s+Ha', parser.line).groups()
        except AttributeError:
            continue
        if key in ['Total energy',
                   'MBD@rsSCS energy',
                   'van der Waals energy corr.']:
            enes[key] = float(val)
    return False


@aims_parser.add('decomposition of the XC Energy')
def get_xc(parser):
    while 'End decomposition' not in parser.readline():
        try:
            key, val = \
                re.search(r'\s*(.*?)\s*\:\s+([-\.\d]+)\s+Ha', parser.line).groups()
        except AttributeError:
            continue
        parser.results['energies'][key] = float(val)


@aims_parser.add('Detailed time accounting')
def get_timing(parser):
    times = {}
    while 'Have a nice day' not in parser.readline():
        regex = r'\| (.*\w)\s+\:\s+([\d\.]+) s'
        m = re.search(regex, parser.line)
        if m:
            label, tm = m.groups()
            times[label] = float(tm)
    parser.results['timing'] = times
