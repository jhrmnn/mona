from caflib.Utils import groupby


def dgroupby(lst, keys):
    if isinstance(keys, str):
        keys = keys.split()

    def keyf(row):
        return tuple(row[key] for key in keys)
    for group_key, group in groupby(lst, key=keyf):
        yield {key: val for key, val in zip(keys, group_key)}, group
