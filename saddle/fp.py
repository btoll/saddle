import functools


def compose(*fs):
    return functools.reduce(compose2, fs)


def compose2(f, g):
    return lambda *a, **kw: f(g(*a, **kw))


def first(l):
    return l[0]

