# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import csv
import json
import os
from collections import OrderedDict
from copy import deepcopy
from importlib import resources
from io import StringIO
from itertools import chain, product, repeat
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
    List,
    Sized,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from .. import sci

if TYPE_CHECKING:
    import numpy as np  # type: ignore
else:
    np = None  # lazy-loaded in Molecule constructor

__version__ = '0.1.0'
__all__ = ['Atom', 'Molecule', 'Crystal', 'readfile', 'load', 'loads']

Vec = Tuple[float, float, float]
_M = TypeVar('_M', bound='Molecule')

bohr = 0.529_177_210_92
with resources.open_text(sci, 'atom-data.csv') as f:
    species_data = OrderedDict(
        (r['symbol'], {**r, 'number': int(r['number'])})  # type: ignore
        for r in csv.DictReader((l for l in f), quoting=csv.QUOTE_NONNUMERIC)
    )
_string_cache: Dict[Any, str] = {}


def no_neg_zeros(r: Any) -> Any:
    return [0.0 if abs(x) < 1e-8 else x for x in r]


class Atom:
    """Represents a single atom.

    :param species: atom type
    :param coord: atom coordinate
    """

    def __init__(self, species: str, coord: Vec, **flags: Any) -> None:
        self.species = species
        self.coord: Vec = cast(Vec, tuple(coord))
        self.flags = flags

    @property
    def mass(self) -> float:
        """Atom mass."""
        mass: float = species_data[self.species]['mass']
        return mass

    @property
    def number(self) -> int:
        """Atom number."""
        return int(species_data[self.species]['number'])

    @property
    def covalent_radius(self) -> float:
        """Covalent radius."""
        r: float = species_data[self.species]['covalent radius']
        return r

    def copy(self) -> 'Atom':
        """Create a copy."""
        return Atom(self.species, self.coord, **deepcopy(self.flags))


class Molecule(Sized, Iterable[Atom]):
    """Represents a molecule.

    :param atoms: atoms
    """

    def __init__(self, atoms: List[Atom], **flags: Any) -> None:
        global np
        if np is None:
            import numpy as np
        self._atoms = atoms
        self.flags = flags

    @classmethod
    def from_coords(
        cls: Type[_M], species: List[str], coords: List[Vec], **flags: Any
    ) -> _M:
        """Alternative constructor.

        :param species: atom types
        :param coords: coordinates
        """
        return cls([Atom(sp, coord) for sp, coord in zip(species, coords)], **flags)

    @property
    def species(self) -> List[str]:
        """Atom types."""
        return [atom.species for atom in self]

    @property
    def numbers(self) -> List[int]:
        """Atom numbers."""
        return [atom.number for atom in self]

    @property
    def mass(self) -> float:
        """Molecular mass."""
        return sum(atom.mass for atom in self)

    @property
    def cms(self) -> Any:
        """Center of mass."""
        masses = np.array([atom.mass for atom in self])
        return (masses[:, None] * self.xyz).sum(0) / self.mass

    @property
    def inertia(self) -> Any:
        """Inertia tensor."""
        masses = np.array([atom.mass for atom in self])
        coords_w = np.sqrt(masses)[:, None] * (self.xyz - self.cms)
        a = np.array([np.diag(np.full(3, r)) for r in np.sum(coords_w ** 2, 1)])
        b = coords_w[:, :, None] * coords_w[:, None, :]
        return np.sum(a - b, 0)

    def __getitem__(self, i: int) -> Atom:
        return self._atoms[i]

    @property
    def coords(self) -> List[Vec]:
        """Coordinates."""
        return [atom.coord for atom in self]

    def __repr__(self) -> str:
        return "<{} '{}'>".format(self.__class__.__name__, self.formula)

    @property
    def xyz(self) -> Any:
        """Coordinates as a numpy array."""
        return np.array(self.coords)

    @property
    def formula(self) -> str:
        """Formula."""
        counter = DefaultDict[str, int](int)
        for species in self.species:
            counter[species] += 1
        return ''.join(f'{sp}{n if n > 1 else ""}' for sp, n in sorted(counter.items()))

    def bondmatrix(self, scale: float) -> Any:
        """Return a connectivity matrix."""
        xyz = self.xyz
        rs = np.array([atom.covalent_radius for atom in self])
        dmatrix = np.sqrt(np.sum((xyz[None, :] - xyz[:, None]) ** 2, 2))
        thrmatrix = scale * (rs[None, :] + rs[:, None])
        return dmatrix < thrmatrix

    def get_fragments(self, scale: float = 1.3) -> List['Molecule']:
        """Return a list of clusters of connected atoms."""
        bond = self.bondmatrix(scale)
        ifragments = getfragments(bond)
        fragments = [
            Molecule([self._atoms[i].copy() for i in fragment])
            for fragment in ifragments
        ]
        return fragments

    def hash(self) -> int:
        """Hash of a molecule from rounded moments of inertia."""
        if len(self) == 1:
            return self[0].number
        return hash(tuple(np.round(sorted(np.linalg.eigvalsh(self.inertia)), 3)))

    def shifted(self: _M, delta: Vec) -> _M:
        """Return a new molecule shifted in space."""
        m = self.copy()
        for atom in m:
            c = atom.coord
            atom.coord = (c[0] + delta[0], c[1] + delta[1], c[2] + delta[2])
        return m

    def __add__(self: _M, other: object) -> _M:
        if not isinstance(other, Molecule):
            return NotImplemented
        geom = self.copy()
        geom._atoms.extend(other.copy())
        return geom

    def centered(self: _M) -> _M:
        """Return a new molecule with a center of mass at origin."""
        return self.shifted(-self.cms)

    def rotated(
        self: _M,
        axis: Union[str, int] = None,
        phi: float = None,
        center: Vec = None,
        rotmat: Any = None,
    ) -> _M:
        """Return a new rotated molecule."""
        if rotmat is None:
            assert axis and phi
            phi = phi * np.pi / 180
            rotmat = np.array(
                [
                    [1, 0, 0],
                    [0, np.cos(phi), -np.sin(phi)],
                    [0, np.sin(phi), np.cos(phi)],
                ]
            )
            if isinstance(axis, str):
                shift = {'x': 0, 'y': 1, 'z': 2}[axis]
            else:
                shift = axis
            for i in [0, 1]:
                rotmat = np.roll(rotmat, shift, i)
        center = np.array(center) if center else self.cms
        m = self.copy()
        for atom in m:
            atom.coord = tuple(center + rotmat.dot(atom.coord - center))  # type: ignore
        return m

    @property
    def centers(self) -> Iterator[Atom]:
        """Iterate over atoms."""
        yield from self._atoms

    def __iter__(self) -> Iterator[Atom]:
        yield from (atom for atom in self._atoms if not atom.flags.get('ghost'))

    def __len__(self) -> int:
        return len([atom for atom in self._atoms if not atom.flags.get('ghost')])

    def __format__(self, fmt: str) -> str:
        fp = StringIO()
        self.dump(fp, fmt)
        return fp.getvalue()

    def items(self) -> Iterator[Tuple[str, Vec]]:
        """Iterate over tuples of atom type and coordinate."""
        for atom in self:
            yield atom.species, atom.coord

    dumps = __format__

    def dump(self, f: IO[str], fmt: str) -> None:
        """Write a molecule to a file.

        Supported formats: 'xyz', 'aims', 'mopac'.
        """
        if fmt == '':
            f.write(repr(self))
        elif fmt == 'xyz':
            f.write('{}\n'.format(len(self)))
            f.write('Formula: {}\n'.format(self.formula))
            for species, coord in self.items():
                f.write(
                    '{:>2} {}\n'.format(
                        species,
                        ' '.join('{:15.8}'.format(x) for x in no_neg_zeros(coord)),
                    )
                )
        elif fmt == 'aims':
            for i, atom in enumerate(self.centers):
                species, r = atom.species, atom.coord
                ghost = atom.flags.get('ghost', False)
                key = (species, r, ghost, fmt)
                r = no_neg_zeros(r)
                try:
                    f.write(_string_cache[key])
                except KeyError:
                    kind = 'atom' if not ghost else 'empty'
                    s = f'{kind} {r[0]:15.8f} {r[1]:15.8f} {r[2]:15.8f} {species:>2}\n'
                    f.write(s)
                    _string_cache[key] = s
                for con in self.flags.get('constrains', {}).get(i, []):
                    f.write(f'constrain_relaxation {con}\n')
        elif fmt == 'mopac':
            f.write('* Formula: {}\n'.format(self.formula))
            for species, coord in self.items():
                f.write(
                    '{:>2} {}\n'.format(
                        species,
                        ' '.join('{:15.8} 1'.format(x) for x in no_neg_zeros(coord)),
                    )
                )
        else:
            raise ValueError("Unknown format: '{}'".format(fmt))

    def copy(self: _M) -> _M:
        """Create a cpoy."""
        return type(self)([atom.copy() for atom in self._atoms])

    def ghost(self: _M) -> _M:
        """Create a copy with all atoms as ghost atoms."""
        m = self.copy()
        for atom in m:
            atom.flags['ghost'] = True
        return m

    def write(self, filename: str) -> None:
        """Write to a file."""
        ext = os.path.splitext(filename)[1]
        if ext == '.xyz':
            fmt = 'xyz'
        elif ext == '.xyzc':
            fmt = 'xyzc'
        elif ext == '.aims' or os.path.basename(filename) == 'geometry.in':
            fmt = 'aims'
        elif ext == '.mopac':
            fmt = 'mopac'
        with open(filename, 'w') as f:
            self.dump(f, fmt)


class Crystal(Molecule):
    """Represents a crystal. Inherits from :class:`Molecule`.

    :param atoms: atoms
    :param lattice: lattice vectors
    """

    def __init__(self, atoms: List[Atom], lattice: List[Vec], **flags: Any) -> None:
        super().__init__(atoms, **flags)
        self.lattice = lattice

    @classmethod
    def from_coords(  # type: ignore
        cls, species: List[str], coords: List[Vec], lattice: List[Vec], **flags
    ) -> 'Crystal':
        """Alternative constructor.

        :param species: atom types
        :param coords: coordinates
        :param lattice: lattice vectors
        """
        return cls(
            [Atom(sp, coord) for sp, coord in zip(species, coords)], lattice, **flags
        )

    def dump(self, f: IO[str], fmt: str) -> None:
        """Write a crystal to a file.

        Supported formats: 'aims', 'vasp'.
        """
        if fmt == '':
            f.write(repr(self))
        elif fmt == 'aims':
            for label, r in zip('abc', self.lattice):
                x, y, z = no_neg_zeros(r)
                f.write(f'lattice_vector {x:15.8f} {y:15.8f} {z:15.8f}\n')
                for con in self.flags.get('constrains', {}).get(label, []):
                    f.write(f'constrain_relaxation {con}\n')
            super().dump(f, fmt)
        elif fmt == 'vasp':
            f.write(f'Formula: {self.formula}\n')
            f.write(f'{1:15.8f}\n')
            for r in self.lattice:
                x, y, z = no_neg_zeros(r)
                f.write(f'{x:15.8f} {y:15.8f} {z:15.8f}\n')
            species: Dict[str, List[Atom]] = OrderedDict(
                (sp, []) for sp in set(self.species)
            )
            f.write(' '.join(species.keys()) + '\n')
            for atom in self:
                species[atom.species].append(atom)
            f.write(' '.join(str(len(atoms)) for atoms in species.values()) + '\n')
            f.write('cartesian\n')
            for atom in chain(*species.values()):
                r = no_neg_zeros(atom.coord)
                s = f'{r[0]:15.8f} {r[1]:15.8f} {r[2]:15.8f}\n'
                f.write(s)
        else:
            raise ValueError(f'Unknown format: {fmt!r}')

    def copy(self) -> 'Crystal':
        """Create a copy."""
        return Crystal([atom.copy() for atom in self._atoms], self.lattice.copy())

    def rotated(
        self,
        axis: Union[str, int] = None,
        phi: float = None,
        center: Vec = None,
        rotmat: Any = None,
    ) -> 'Crystal':
        """Return a new crystal with rotated unit cell and lattice vectors."""
        assert center is None
        g = super().rotated(axis, phi, (0, 0, 0), rotmat)
        m = Molecule.from_coords(['_'] * 3, self.lattice)
        m = m.rotated(axis, phi, (0, 0, 0), rotmat)
        g.lattice = m.coords
        return g

    @property
    def abc(self) -> Any:
        """Latice vectors as a numpy array."""
        return np.array(self.lattice)

    def get_kgrid(self, density: float = 0.06) -> Tuple[int, int, int]:
        """Return a k-point grid with a given density."""
        rec_lattice = 2 * np.pi * np.linalg.inv(self.abc.T)
        rec_lens = np.sqrt((rec_lattice ** 2).sum(1))
        nkpts = np.ceil(rec_lens / (density * bohr))
        return int(nkpts[0]), int(nkpts[1]), int(nkpts[2])

    def supercell(self, ns: Tuple[int, int, int]) -> 'Crystal':
        """Create a supercell."""
        abc = self.abc
        latt_vectors = np.array(
            [
                sum(s * vec for s, vec in zip(shift, abc))
                for shift in product(*map(range, ns))
            ]
        )
        species = list(chain.from_iterable(repeat(self.species, len(latt_vectors))))
        coords = [
            (x, y, z)
            for x, y, z in (self.xyz[None, :, :] + latt_vectors[:, None, :]).reshape(
                (-1, 3)
            )
        ]
        lattice = [(x, y, z) for x, y, z in abc * np.array(ns)[:, None]]
        return Crystal.from_coords(species, coords, lattice)

    def normalized(self) -> 'Crystal':
        """Create a copy with atoms on unit cell faces normalized."""
        xyz = (
            np.mod(self.xyz @ np.linalg.inv(self.lattice) + 1e-10, 1) - 1e-10
        ) @ self.lattice
        return Crystal.from_coords(self.species, xyz, self.lattice.copy())


def get_vec(ws: List[str]) -> Vec:
    return float(ws[0]), float(ws[1]), float(ws[2])


def load(fp: IO[str], fmt: str) -> Molecule:  # noqa: C901
    """Read a molecule or a crystal from a file object."""
    if fmt == 'xyz':
        n = int(fp.readline())
        try:
            flags = json.loads(fp.readline())
        except json.decoder.JSONDecodeError:
            flags = {}
        species = []
        coords = []
        for _ in range(n):
            ws = fp.readline().split()
            species.append(ws[0])
            coords.append(get_vec(ws[1:4]))
        return Molecule.from_coords(species, coords, **flags)
    elif fmt == 'xyzc':
        n = int(fp.readline())
        lattice = []
        for _ in range(3):
            lattice.append(get_vec(fp.readline().split()))
        species = []
        coords = []
        for _ in range(n):
            ws = fp.readline().split()
            species.append(ws[0])
            coords.append(get_vec(ws[1:4]))
        return Crystal.from_coords(species, coords, lattice)
    if fmt == 'aims':
        atoms = []
        lattice = []
        while True:
            line = fp.readline()
            if line == '':
                break
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            ws = line.split()
            what = ws[0]
            if what in ['atom', 'empty']:
                atoms.append(Atom(ws[4], get_vec(ws[1:4]), ghost=what == 'empty'))
            elif what == 'lattice_vector':
                lattice.append(get_vec(ws[1:4]))
        if lattice:
            assert len(lattice) == 3
            return Crystal(atoms, lattice)
        else:
            return Molecule(atoms)
    raise ValueError(f'Unknown format: {fmt}')


def loads(s: str, fmt: str) -> Molecule:
    """Read a molecule or a crystal from a string."""
    fp = StringIO(s)
    return load(fp, fmt)


def readfile(path: str, fmt: str = None) -> Molecule:
    """Read a molecule or a crystal from a path."""
    if not fmt:
        ext = os.path.splitext(path)[1]
        if ext == '.xyz':
            fmt = 'xyz'
        elif ext == '.aims' or os.path.basename(path) == 'geometry.in':
            fmt = 'aims'
        elif ext == '.xyzc':
            fmt = 'xyzc'
        else:
            raise RuntimeError('Cannot determine format')
    with open(path) as f:
        return load(f, fmt)


def getfragments(conn: Any) -> List[List[int]]:
    n = conn.shape[0]
    assigned = [-1 for _ in range(n)]  # fragment index, otherwise -1
    ifragment = 0  # current fragment index
    queue = [0 for _ in range(n)]  # allocate queue of neighbors
    for elem in range(n):  # iterate over elements
        if assigned[elem] >= 0:  # skip if assigned
            continue
        queue[0], a, b = elem, 0, 1  # queue starting with the element itself
        while b - a > 0:  # until queue is exhausted
            node, a = queue[a], a + 1  # pop from queue
            assigned[node] = ifragment  # assign node
            neighbors = np.flatnonzero(conn[node, :])  # list of neighbors
            for neighbor in neighbors:
                if not (assigned[neighbor] >= 0 or neighbor in queue[a:b]):
                    # add to queue if not assigned or in queue
                    queue[b], b = neighbor, b + 1
        ifragment += 1
    fragments = [
        [i for i, f in enumerate(assigned) if f == fragment]
        for fragment in range(ifragment)
    ]
    return fragments
