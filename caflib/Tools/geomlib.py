# Copyright (c) 2017 Jan Hermann
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from collections import defaultdict, OrderedDict
from itertools import chain, product, groupby
from functools import cmp_to_key
import json
from pathlib import Path
import csv
import os
from io import StringIO
import numpy as np  # type: ignore
from numpy import sin, cos

from typing import (
    Iterable, Dict, Any, Tuple, cast, Union, List, Iterator, IO, ClassVar,
    Sized, DefaultDict
)

settings = {
    'precision': 8,
    'width': 15,
    'eq_precision': 10
}

geom_formats = {
    'xyz': 'xyz',
    'aims': 'aims',
    'vasp': 'vasp',
    'json': 'json'
}

bohr = 0.52917721092


def scalar2str(x: float) -> str:
    return f'{x:{settings["width"]}.{settings["precision"]}f}'


def vector2str(v: Iterable[float]) -> str:
    return ' '.join(scalar2str(x) for x in v)


def cmp3d(x: np.ndarray, y: np.ndarray) -> int:
    thre = 10**-settings['eq_precision']
    for i in range(3):
        diff = x[i]-y[i]
        if abs(diff) > thre:
            return int(np.sign(diff))
    return 0


Coord = Union[Tuple[float, ...], List[float]]


class Atom:
    data: ClassVar[Dict[str, Dict[str, Any]]]
    # Atom.data is defined at the end of this file for readability

    def __init__(self, specie: str, coord: Coord, flags: Dict[str, Any] = None) -> None:
        self.specie = specie.capitalize()
        self.number: int = Atom.data[self.specie]['number']
        self.coord = np.array(coord, float)
        self.flags = flags or {}

    def __repr__(self) -> str:
        return 'Atom({!r}, {}, flags={!r})'.format(
            self.specie,
            '({})'.format(
                ', '.join(f'{x:.{settings["precision"]}}' for x in self.coord)
            ),
            self.flags
        )

    def __format__(self, fmt: str) -> str:
        if fmt == 'xyz':
            return f'{self.specie:>2} {vector2str(self.coord)}'
        if fmt == 'aims':
            if self.flags.get('dummy'):
                name = 'empty'
            else:
                name = 'atom'
            s = f'{name} {vector2str(self.coord)} {self.specie:>2}'
            for c in self.flags.get('constrained', []):
                s += f'\nconstrain_relaxation {c}'
            return s
        return super().__format__(fmt)

    def prop(self, name: str) -> Any:
        return Atom.data[self.specie][name]

    @property
    def mass(self) -> float:
        return cast(float, self.prop('mass'))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Atom):
            return NotImplemented  # type:ignore
        if self.specie != self.specie:
            return False
        return cmp3d(self.coord, other.coord) == 0

    def copy(self) -> 'Atom':
        return Atom(self.specie, self.coord.copy(), self.flags.copy())

    def dist(self, other: Union['Atom', np.ndarray, Iterable['Atom']]) -> float:
        if isinstance(other, Atom):
            return self.dist(other.coord)
        try:
            return cast(float, np.linalg.norm(self.coord-np.array(other)))
        except:
            pass
        try:
            return min(self.dist(atom.coord) for atom in other)
        except:
            pass
        raise TypeError(f'Cannot calculate distance to {other.__class__.__name__!r} object')

    @property
    def group(self) -> int:
        n = self.number
        if n <= 2:
            return 1 if n == 1 else 8
        if n <= 18:
            return ((n-2-1) % 8)+1
        if n <= 54:
            n = ((n-18-1) % 18)+1
            if n <= 2:
                return n
            if n-10 >= 3:
                return n-10
        if n <= 118:
            n = ((n-54-1) % 32)+1
            if n <= 2:
                return n
            if n-24 >= 3:
                return n-24
        raise RuntimeError('Elements above 118 not supported')


class Molecule(Iterable, Sized):
    def __init__(self, atoms: List[Atom], flags: Dict[str, Any] = None) -> None:
        self.atoms = atoms
        self.flags = flags or {}

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} {self.formula!r}>'

    def __iter__(self) -> Iterator[Atom]:
        yield from self.atoms

    def __format__(self, fmt: str) -> str:
        fp = StringIO()
        self.dump(fp, fmt)
        return fp.getvalue()

    def __contains__(self, item: object) -> bool:
        if not isinstance(item, str):
            return NotImplemented  # type: ignore
        return any(item == a.specie for a in self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Molecule):
            return NotImplemented  # type:ignore
        cmp = lambda a, b: cmp3d(a.coord, b.coord)
        key = cmp_to_key(cmp)
        return all(a == b for a, b in zip(
            sorted(list(self), key=key),
            sorted(list(other), key=key)
        ))

    def __add__(self, other: object) -> 'Molecule':
        if not isinstance(other, Molecule):
            return NotImplemented  # type:ignore
        return Molecule(self.atoms + other.atoms)

    def __len__(self) -> int:
        return len(self.atoms)

    def __getitem__(self, idx: Union[str, int]) -> Any:
        if isinstance(idx, int):
            return self.atoms[idx]
        return self.flags[idx]

    def __setitem__(self, key: str, value: Any) -> None:
        self.flags[key] = value

    def copy(self) -> 'Molecule':
        return Molecule([atom.copy() for atom in self], self.flags.copy())

    def items(self) -> Iterator[Tuple[str, Coord]]:
        for atom in self:
            yield atom.specie, cast(Coord, tuple(atom.coord))

    dumps = __format__

    def dump(self, fp: IO[str], fmt: str) -> None:
        if fmt == 'xyz':
            fp.write(f'{len(self)}\n')
            json.dump({'formula': self.formula, **self.flags}, fp)
            fp.write('\n')
            for atom in self:
                fp.write(f'{atom:xyz}\n')
        elif fmt == 'aims':
            for atom in self:
                fp.write(f'{atom:aims}\n')
        elif fmt == 'json':
            json.dump({'atoms': [[a.specie, list(a.coord)] for a in self]}, fp)
        else:
            raise ValueError(f'Unknown format: {fmt!r}')

    def write(self, path: os.PathLike, fmt: str = None) -> None:
        path = Path(path)
        if not fmt:
            fmt = geom_formats.get(path.suffix[1:])
        if not fmt:
            if path.name.endswith('geometry.in'):
                fmt = 'aims'
            else:
                raise ValueError(f'Unknown format: {fmt!r}')
        with path.open('w') as f:
            self.dump(f, fmt)

    @property
    def formula(self) -> str:
        counter = DefaultDict[str, int](int)
        for specie in self.species:
            counter[specie] += 1
        return ''.join(
            f'{sp}{n if n > 1 else ""}' for sp, n in sorted(counter.items())
        )

    @property
    def coords(self) -> np.ndarray:
        return np.array([a.coord for a in self])

    @property
    def species(self) -> List[str]:
        return [a.specie for a in self]

    @property
    def mass(self) -> float:
        return sum(atom.mass for atom in self)

    @property
    def cms(self) -> np.ndarray:
        return sum(atom.mass*atom.coord for atom in self)/self.mass

    @property
    def bounding_box(self) -> Tuple[np.ndarray, np.ndarray]:
        coords = self.coords
        return coords.min(0), coords.max(0)

    @property
    def dimensions(self) -> np.ndarray:
        bb = self.bounding_box
        return bb[1]-bb[0]

    @property
    def inertia(self) -> np.ndarray:
        masses = np.array([atom.mass for atom in self])
        coords_w = np.sqrt(masses)[:, None]*self.shifted(-self.cms).coords
        A = np.array([np.diag(np.full(3, r)) for r in np.sum(coords_w**2, 1)])
        B = coords_w[:, :, None]*coords_w[:, None, :]
        return np.sum(A-B, 0)

    @property
    def moments(self) -> List[float]:
        return sorted(np.linalg.eigvals(self.inertia))

    def shifted(self, delta: Union[Coord, np.ndarray]) -> 'Molecule':
        m = self.copy()
        for atom in m:
            atom.coord += delta
        return m

    def part(self, idxs: List[int]) -> 'Molecule':
        return Molecule([self[idx-1].copy() for idx in idxs])

    def rotated(
            self,
            axis: Union[str, int] = None,
            phi: float = None,
            center: Union[np.ndarray, Coord] = None,
            rotmat: np.ndarray = None
    ) -> 'Molecule':
        if rotmat is None:
            assert axis and phi
            phi = phi*np.pi/180
            rotmat = np.array(
                [1, 0, 0,
                 0, cos(phi), -sin(phi),
                 0, sin(phi), cos(phi)]
            ).reshape(3, 3)
            if isinstance(axis, str):
                shift = {'x': 0, 'y': 1, 'z': 2}[axis]
            else:
                shift = axis
            for i in [0, 1]:
                rotmat = np.roll(rotmat, shift, i)
        center = np.array(center) if center else self.cms
        m = self.copy()
        for atom in m:
            atom.coord = center+rotmat.dot(atom.coord-center)
        return m

    def bondmatrix(self, scale: float) -> np.ndarray:
        coords = self.coords
        Rs = np.array([atom.prop('covalent radius') for atom in self])
        dmatrix = np.sqrt(np.sum((coords[None, :]-coords[:, None])**2, 2))
        thrmatrix = scale*(Rs[None, :]+Rs[:, None])
        return dmatrix < thrmatrix

    def draw(self, method: str = 'imolecule', **kwargs: Any) -> None:
        bond = self.bondmatrix(1.3)
        if method == 'imolecule':
            import imolecule  # type: ignore
            obj = {
                'atoms': [
                    {'element': atom.specie, 'location': atom.coord.tolist()}
                    for atom in self
                ],
                'bonds': [
                    {'atoms': [i, j], 'order': 1}
                    for i in range(len(self))
                    for j in range(i)
                    if bond[i, j]
                ]
            }
            imolecule.draw(obj, 'json', **kwargs)

    def dist(self, obj: Union[Atom, np.ndarray, Iterable[Atom]]) -> float:
        return min(atom.dist(obj) for atom in self)

    def get_fragments(self, scale: float = 1.3) -> List['Molecule']:
        bond = self.bondmatrix(scale)
        ifragments = getfragments(bond)
        fragments = [
            Molecule([self[i].copy() for i in fragment])
            for fragment in ifragments
        ]
        return fragments


def getfragments(C: np.ndarray) -> List[List[int]]:
    """Find fragments within a set of sparsely connected elements.

    Given square matrix C where C_ij = 1 if i and j are connected
    and 0 otherwise, it extends the connectedness (if i and j and j and k
    are connected, i and k are also connected) and returns a list sets of
    elements which are not connected by any element.

    The algorithm visits all elements, checks whether it wasn't already
    assigned to a fragment, if not, it crawls it's neighbors and their
    neighbors etc., until it cannot find any more neighbors. Then it goes
    to the next element until all were visited.
    """
    n = C.shape[0]
    assigned = [-1 for _ in range(n)]  # fragment index, otherwise -1
    ifragment = 0  # current fragment index
    queue = [0 for _ in range(n)]  # allocate queue of neighbors
    for elem in range(n):  # iterate over elements
        if assigned[elem] >= 0:  # skip if assigned
            continue
        queue[0], a, b = elem, 0, 1  # queue starting with the element itself
        while b-a > 0:  # until queue is exhausted
            node, a = queue[a], a+1  # pop from queue
            assigned[node] = ifragment  # assign node
            neighbors = np.flatnonzero(C[node, :])  # list of neighbors
            for neighbor in neighbors:
                if not (assigned[neighbor] >= 0 or neighbor in queue[a:b]):
                    # add to queue if not assigned or in queue
                    queue[b], b = neighbor, b+1
        ifragment += 1
    fragments = [[i for i, f in enumerate(assigned) if f == fragment]
                 for fragment in range(ifragment)]
    return fragments


class Crystal(Molecule):
    def __init__(
            self,
            atoms: List[Atom],
            lattice: np.ndarray,
            flags: Dict[str, Any] = None
    ) -> None:
        super().__init__(atoms, flags)
        self.lattice = np.array(lattice)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Crystal):
            return NotImplemented  # type:ignore
        return np.linalg.norm(self.lattice-other.lattice) < settings['real_eq'] and \
            super().__eq__(other)

    def copy(self) -> 'Crystal':
        return Crystal([a.copy() for a in self], self.lattice.copy(), self.flags.copy())

    @classmethod
    def from_molecule(cls, mol: Molecule, padding: float = 3.) -> 'Crystal':
        bb = mol.bounding_box
        X1 = bb[0]-padding
        X2 = bb[1]+padding
        dims = X2-X1
        mol = mol.shifted(-X1)
        return cls(np.diag(dims), mol.atoms)

    def supercell(self, ns: Tuple[int, int, int]) -> 'Crystal':
        atoms = []
        for shift in product(*[range(n) for n in ns]):
            for atom in self:
                atom = atom.copy()
                atom.coord += sum(k*v for k, v in zip(shift, self.lattice))
                atom.flags['cell'] = shift
                atoms.append(atom)
        return Crystal(atoms, np.array(ns)[:, None]*self.lattice, self.flags.copy())

    def get_kgrid(self, density: float = 0.06) -> Tuple[int, int, int]:
        rec_lattice = 2*np.pi*np.linalg.inv(self.lattice.T)
        rec_lens = np.sqrt((rec_lattice**2).sum(1))
        nkpts = np.ceil(rec_lens/(density*bohr))
        return int(nkpts[0]), int(nkpts[1]), int(nkpts[2])

    def complete_molecules(self) -> 'Crystal':
        def key(x: Atom) -> Tuple[int, int, int]:
            return x.flags['cell']  # type: ignore
        central = []
        for frag in self.supercell((3, 3, 3)).get_fragments():
            atoms = sorted(frag.atoms, key=key)
            groups = [(cell, list(g)) for cell, g in groupby(atoms, key=key)]
            highest = max(len(g) for cell, g in groups)
            first = [cell for cell, g in groups if len(g) == highest][0]
            if first == (1, 1, 1):
                central.append(frag)
        return Crystal(
            concat(central).shifted(-sum(self.lattice)).atoms,
            self.lattice,
            self.flags
        )

    def dump(self, fp: IO[str], fmt: str) -> None:
        if fmt == 'aims':
            for i, l in enumerate(self.lattice):
                fp.write(f'lattice_vector {vector2str(l)}\n')
                for c in self.flags.get('constrained', {}).get(i, []):
                    fp.write(f'constrain_relaxation {c}\n')
            for atom in self:
                fp.write(f'{atom:aims}\n')
        elif fmt == 'vasp':
            fp.write(f'Formula: {self.formula}\n')
            fp.write(scalar2str(1) + '\n')
            for l in self.lattice:
                fp.write(vector2str(l) + '\n')
            species: Dict[str, List[Atom]] = OrderedDict((sp, []) for sp in set(self.species))
            fp.write(' '.join(species.keys()) + '\n')
            for atom in self:
                species[atom.specie].append(atom)
            fp.write(' '.join(str(len(atoms)) for atoms in species.values()) + '\n')
            fp.write('cartesian\n')
            for atom in chain(*species.values()):
                fp.write(vector2str(atom.coord) + '\n')
        elif fmt == 'json':
            json.dump({
                'atoms': [[a.specie, a.coord.tolist()] for a in self],
                'lattice': self.lattice.tolist()
            }, fp)
        else:
            raise ValueError(f'Unknown format: {fmt!r}')


def concat(objs: Iterable[Molecule]) -> Molecule:
    return sum(objs, Molecule([]))


def load(fp: IO[str], fmt: str) -> Molecule:
    if fmt == 'xyz':
        n = int(fp.readline())
        comment = fp.readline().strip()
        try:
            flags = json.loads(comment)
        except json.decoder.JSONDecodeError:
            flags = {'comment': comment} if comment else {}
        atoms = []
        for _ in range(n):
            ws = fp.readline().split()
            atoms.append(Atom(ws[0], tuple(float(x) for x in ws[1:4])))
        return Molecule(atoms, flags=flags)
    elif fmt == 'aims':
        atoms = []
        atoms_frac = []
        lattice_vecs = []
        while True:
            l = fp.readline()
            if not l:
                break
            l = l.strip()
            if not l or l.startswith('#'):
                continue
            ws = l.split()
            what = ws[0]
            if what == 'atom':
                atoms.append(Atom(ws[4], [float(x) for x in ws[1:4]]))
            elif what == 'atom_frac':
                atoms_frac.append((ws[4], [float(x) for x in ws[1:4]]))
            elif what == 'lattice_vector':
                lattice_vecs.append([float(x) for x in ws[1:4]])
        if lattice_vecs:
            assert len(lattice_vecs) == 3
            lattice = np.array(lattice_vecs)
            for sp, coord in atoms_frac:
                atoms.append(Atom(sp, lattice.dot(coord)))
            return Crystal(atoms, lattice)
        else:
            return Molecule(atoms)
    elif fmt == 'vasp':
        fp.readline()
        scale = float(fp.readline())
        lattice = scale*np.array([
            [float(x) for x in fp.readline().split()]
            for _ in range(3)])
        species = fp.readline().split()
        nspecies = [int(x) for x in fp.readline().split()]
        while True:
            coordtype = fp.readline().strip()[0].lower()
            if coordtype in 'dc':
                break
        if scale != 1:
            assert coordtype == 'd'
        atoms = []
        for sp, n in zip(species, nspecies):
            for _ in range(n):
                xyz = np.array([float(x) for x in fp.readline().split()[:3]])
                if coordtype == 'd':
                    xyz = xyz.dot(lattice)
                atoms.append(Atom(sp, xyz))
        return Crystal(atoms, lattice)
    elif fmt == 'xyzc':
        n = int(fp.readline())
        lattice = np.array([
            [float(x) for x in fp.readline().split()]
            for _ in range(3)])
        atoms = []
        for _ in range(n):
            ws = fp.readline().split()
            atoms.append(Atom(ws[0], [float(x) for x in ws[1:4]]))
        return Crystal(atoms, lattice)
    elif fmt == 'smi':
        import pybel  # type: ignore
        smi = fp.read().strip()
        mol = pybel.readstring('smi', smi)
        mol.addh()
        mol.make3D()
        return loads(mol.write('xyz'), 'xyz')
    else:
        raise ValueError(f'Unknown format: {fmt!r}')


def loads(s: str, fmt: str) -> Molecule:
    fp = StringIO(s)
    return load(fp, fmt)


def readfile(path: os.PathLike, fmt: str = None) -> Molecule:
    path = Path(path)
    if not fmt:
        fmt = geom_formats.get(path.suffix[1:])
    if not fmt:
        if path.name.endswith('geometry.in'):
            fmt = 'aims'
        else:
            raise ValueError(f'Unknown format: {fmt!r}')
    with path.open() as f:
        return load(f, fmt)


Atom.data = OrderedDict(
    (r['symbol'], {**r, 'number': int(r['number'])})
    for r in csv.DictReader(quoting=csv.QUOTE_NONNUMERIC, f=StringIO(  # type: ignore
            """\
"number","symbol","name","vdw radius","covalent radius","mass","ionization energy"
1,"H","hydrogen",1.2,0.38,1.0079,13.5984
2,"He","helium",1.4,0.32,4.0026,24.5874
3,"Li","lithium",1.82,1.34,6.941,5.3917
4,"Be","beryllium",1.53,0.9,9.0122,9.3227
5,"B","boron",1.92,0.82,10.811,8.298
6,"C","carbon",1.7,0.77,12.0107,11.2603
7,"N","nitrogen",1.55,0.75,14.0067,14.5341
8,"O","oxygen",1.52,0.73,15.9994,13.6181
9,"F","fluorine",1.47,0.71,18.9984,17.4228
10,"Ne","neon",1.54,0.69,20.1797,21.5645
11,"Na","sodium",2.27,1.54,22.9897,5.1391
12,"Mg","magnesium",1.73,1.3,24.305,7.6462
13,"Al","aluminium",1.84,1.18,26.9815,5.9858
14,"Si","silicon",2.1,1.11,28.0855,8.1517
15,"P","phosphorus",1.8,1.06,30.9738,10.4867
16,"S","sulfur",1.8,1.02,32.065,10.36
17,"Cl","chlorine",1.75,0.99,35.453,12.9676
18,"Ar","argon",1.88,0.97,39.948,15.7596
19,"K","potassium",2.75,1.96,39.0983,4.3407
20,"Ca","calcium",2.31,1.74,40.078,6.1132
21,"Sc","scandium",2.11,1.44,44.9559,6.5615
22,"Ti","titanium",,1.36,47.867,6.8281
23,"V","vanadium",,1.25,50.9415,6.7462
24,"Cr","chromium",,1.27,51.9961,6.7665
25,"Mn","manganese",,1.39,54.938,7.434
26,"Fe","iron",,1.25,55.845,7.9024
27,"Co","cobalt",,1.26,58.9332,7.881
28,"Ni","nickel",1.63,1.21,58.6934,7.6398
29,"Cu","copper",1.4,1.38,63.546,7.7264
30,"Zn","zinc",1.39,1.31,65.39,9.3942
31,"Ga","gallium",1.87,1.26,69.723,5.9993
32,"Ge","germanium",2.11,1.22,72.64,7.8994
33,"As","arsenic",1.85,1.19,74.9216,9.7886
34,"Se","selenium",1.9,1.16,78.96,9.7524
35,"Br","bromine",1.85,1.14,79.904,11.8138
36,"Kr","krypton",2.02,1.1,83.8,13.9996
37,"Rb","rubidium",3.03,2.11,85.4678,4.1771
38,"Sr","strontium",2.49,1.92,87.62,5.6949
39,"Y","yttrium",,1.62,88.9059,6.2173
40,"Zr","zirconium",,1.48,91.224,6.6339
41,"Nb","niobium",,1.37,92.9064,6.7589
42,"Mo","molybdenum",,1.45,95.94,7.0924
43,"Tc","technetium",,1.56,98,7.28
44,"Ru","ruthenium",,1.26,101.07,7.3605
45,"Rh","rhodium",,1.35,102.9055,7.4589
46,"Pd","palladium",1.63,1.31,106.42,8.3369
47,"Ag","silver",1.72,1.53,107.8682,7.5762
48,"Cd","cadmium",1.58,1.48,112.411,8.9938
49,"In","indium",1.93,1.44,114.818,5.7864
50,"Sn","tin",2.17,1.41,118.71,7.3439
51,"Sb","antimony",2.06,1.38,121.76,8.6084
52,"Te","tellurium",2.06,1.35,127.6,9.0096
53,"I","iodine",1.98,1.33,126.9045,10.4513
54,"Xe","xenon",2.16,1.3,131.293,12.1298
55,"Cs","caesium",3.43,2.25,132.9055,3.8939
56,"Ba","barium",2.68,1.98,137.327,5.2117
57,"La","lanthanum",,1.69,138.9055,5.5769
58,"Ce","cerium",,,140.116,5.5387
59,"Pr","praseodymium",,,140.9077,5.473
60,"Nd","neodymium",,,144.24,5.525
61,"Pm","promethium",,,145,5.582
62,"Sm","samarium",,,150.36,5.6437
63,"Eu","europium",,,151.964,5.6704
64,"Gd","gadolinium",,,157.25,6.1501
65,"Tb","terbium",,,158.9253,5.8638
66,"Dy","dysprosium",,,162.5,5.9389
67,"Ho","holmium",,,164.9303,6.0215
68,"Er","erbium",,,167.259,6.1077
69,"Tm","thulium",,,168.9342,6.1843
70,"Yb","ytterbium",,,173.04,6.2542
71,"Lu","lutetium",,1.6,174.967,5.4259
72,"Hf","hafnium",,1.5,178.49,6.8251
73,"Ta","tantalum",,1.38,180.9479,7.5496
74,"W","tungsten",,1.46,183.84,7.864
75,"Re","rhenium",,1.59,186.207,7.8335
76,"Os","osmium",,1.28,190.23,8.4382
77,"Ir","iridium",,1.37,192.217,8.967
78,"Pt","platinum",1.75,1.28,195.078,8.9587
79,"Au","gold",1.66,1.44,196.9665,9.2255
80,"Hg","mercury",1.55,1.49,200.59,10.4375
81,"Tl","thallium",1.96,1.48,204.3833,6.1082
82,"Pb","lead",2.02,1.47,207.2,7.4167
83,"Bi","bismuth",2.07,1.46,208.9804,7.2856
84,"Po","polonium",1.97,,209,8.417
85,"At","astatine",2.02,,210,9.3
86,"Rn","radon",2.2,1.45,222,10.7485
87,"Fr","francium",3.48,,223,4.0727
88,"Ra","radium",2.83,,226,5.2784
89,"Ac","actinium",,,227,5.17
90,"Th","thorium",,,232.0381,6.3067
91,"Pa","protactinium",,,231.0359,5.89
92,"U","uranium",1.86,,238.0289,6.1941"""))
)
