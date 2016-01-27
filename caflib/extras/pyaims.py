import xml.etree.cElementTree as ET
import numpy as np
import re
try:
    from caflib.extras.logparser import Parser
except ImportError:
    from logparser import Parser


def parse_xml(source):
    root = ET.parse(source).getroot()
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
        if typename == 'dble':
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
        return np.array([typef(x) for x in xmlarr.text.split()])


pat_junk = re.compile(
    r'''
    [|,]|            # pipe or comma
    (?<!\d)\.(?!\W)  # dot if not preceeded by \d and not succeeded by \W
    ''', re.VERBOSE)
pat_spaces = re.compile(
    r'''
    \s*:\s+|  # colon preceeded by zero or more spaces and followed by at least one
    \s{2,}    # two or more spaces
    ''', re.VERBOSE)


def hook(s):
    s = re.sub(pat_junk, '', s).strip()
    s = re.sub(pat_spaces, '\t', s)
    return s


aims_parser = Parser(hook)


def parse_output(source):
    return aims_parser.parse(source)


@aims_parser.add('The structure contains')
def get_atoms(parser):
    words = parser.line.split('\t')
    parser.results['n_atoms'] = int(words[1].split()[0])
    parser.results['n_elec'] = float(words[3].split()[0])


@aims_parser.add('Self-consistency cycle converged')
def set_converged(parser):
    parser.results['converged'] = True


@aims_parser.add('Performing Hirshfeld analysis')
def get_hirsh(parser):
    parser.readline()
    atoms = []
    while parser.readline():
        atom = {}
        atom['element'] = parser.line.split('\t')[2]
        while '---' not in parser.readline():
            if not re.match(r'\w', parser.line[0]):
                continue
            words = parser.line.split('\t')
            key = words[0]
            val = [float(x) for x in words[1:]]
            val = val[0] if len(val) == 1 else np.array(val)
            atom[key] = val
        atoms.append(atom)
    parser.results['Hirshfeld'] = atoms


@aims_parser.add('Many-Body Dispersion')
def get_mbd(parser):
    parser.readline()
    while 'omega' not in parser.readline():
        pass
    labels = parser.line.split('\t')
    rows = []
    while '---' not in parser.readline():
        rows.append([float(x) for x in parser.line.split('\t')])
    alpha = {lab: c for lab, c in zip(labels, np.array(rows).T)}
    parser.results['MBD']['dynamic polarizability'] = alpha
    while '---' not in parser.readline():
        pass
    atoms = []
    while '---' not in parser.readline():
        words = re.split(r'\s+', parser.line)
        atoms.append({'elem': words[2],
                      'C6': float(words[3]),
                      'alpha': float(words[4])})
    parser.results['MBD']['partitioned C6'] = atoms


@aims_parser.add('Total energy components')
def get_energy(parser):
    if not parser.results['converged']:
        return
    while parser.readline():
        if '---' in parser.line:
            continue
        key, val = parser.line.split('\t')[:2]
        if key in ['Total energy',
                   'MBD@rsSCS energy',
                   'van der Waals energy corr.']:
            parser.results['energies'][key] = float(val.split()[0])


@aims_parser.add('Meta-GGA post processing starts')
def get_metagga_energy(parser):
    while 'Meta-gga total energy' not in parser.readline(hook):
        pass
    name, val = parser.line.split('\t')[0:2]
    val = float(val.split()[0])
    parser.results['energies'][name] = val


@aims_parser.add('decomposition of the XC Energy')
def get_xc(parser):
    while 'End decomposition' not in parser.readline():
        if '---' in parser.line:
            continue
        words = parser.line.split('\t')
        if len(words) == 1:
            continue
        parser.results['energies'][words[0]] = float(words[1].split()[0])


@aims_parser.add('Detailed time accounting')
def get_timing(parser):
    while parser.readline():
        words = parser.line.split('\t')
        try:
            parser.results['timing'][words[0]] = float(words[1].split()[0])
        except ValueError:
            pass
