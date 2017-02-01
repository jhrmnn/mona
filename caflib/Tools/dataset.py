# from itertools import groupby


class Dataset:
    def __init__(self, name):
        self.name = name
        self.geoms = {}
        self.clusters = {}
        self.depth = None

    def __repr__(self):
        return '<Dataset "{}" containing {} clusters and {} structures>'.format(
            self.name, len(self.clusters), len(self.geoms)
        )

    def get_task(self, ctx, taskgen):
        tasks = {
            geomid: taskgen(ctx, geom, self.name)
            for geomid, geom in self.geoms.items()
        }
        tasktree = [(
            key,
            [
                tasks[geomid] + ctx.link(fragment)
                for fragment, geomid
                in cluster.fragments.items()
                if tasks[geomid]
            ] + ctx()
        ) for key, cluster in self.clusters.items()]
        tasktree.sort(key=lambda x: x[0])
        return [
            task + ctx.link('_'.join(str(k) for k in key))
            for key, task in tasktree
        ] + ctx()

    def __setitem__(self, key, value):
        if self.depth is None:
            self.depth = len(key)
        else:
            assert len(key) == self.depth
        self.clusters[key] = value

    def get_int_enes(self, energies, scale=1):
        return {
            key: cluster.get_int_ene(energies[key])*scale
            for key, cluster in self.clusters.items()
            if key in energies
        }


class Cluster:
    def __init__(self, fragments=None, energies=None, intene=None):
        self.fragments = fragments or {}
        self.energies = energies or {}
        self._intene = intene

    def __repr__(self):
        return f'Cluster({self.fragments!r})'

    def __setitem__(self, key, value):
        self.fragments[key] = value

    def get_int_ene(self, energies):
        assert self.fragments.keys() == energies.keys()
        return self._intene(energies)
