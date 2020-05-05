class _ANY:
    def __eq__(self, a):
        return True

    def __repr__(self):
        return type(self).__name__


ANY = _ANY()


class _ANYLIST(_ANY):
    def __eq__(self, a):
        return isinstance(a, list)


ANYLIST = _ANYLIST()


class _ANYSTRING(_ANY):
    def __eq__(self, a):
        return isinstance(a, str)


ANYSTRING = _ANYSTRING()


class _ANYDICT(_ANY):
    def __eq__(self, a):
        return isinstance(a, dict)


ANYDICT = _ANYDICT()


class _ANYBOOL(_ANY):
    def __eq__(self, a):
        return isinstance(a, bool)


ANYBOOL = _ANYBOOL()


class _ANYINT(_ANY):
    def __eq__(self, a):
        return isinstance(a, int)


ANYINT = _ANYINT()
