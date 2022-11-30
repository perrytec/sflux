class ROW:
    """"""
    def __init__(self, column: str):
        self.column = column

    def __eq__(self, other: str):
        return f'{self} == {self._parse_other(other)}'

    def __ne__(self, other):
        return f'{self} != {self._parse_other(other)}'

    def __ge__(self, other):
        return f'{self} >= {self._parse_other(other)}'

    def __gt__(self, other):
        return f'{self} > {self._parse_other(other)}'

    def __le__(self, other):
        return f'{self} <= {self._parse_other(other)}'

    def __lt__(self, other):
        return f'{self} < {self._parse_other(other)}'

    def in_(self, iterable: (list, tuple)):
        return f'contains(value: {self}, set: {self._parse_other(iterable)})'

    def __repr__(self):
        return f'r["{self.column}"]'

    def _parse_other(self, other):
        if isinstance(other, (list, tuple)):
            return '[' + ','.join([self._parse_other(elem) for elem in other]) + ']'
        if isinstance(other, str):
            return f'"{other}"'
        return str(other)

    # MATH

    def __add__(self, other):
        return _RowOp(f'({self} + {other})')

    def __sub__(self, other):
        return _RowOp(f'({self} - {other})')

    def __mul__(self, other):
        return _RowOp(f'{self} * {other}')

    def __truediv__(self, other):
        return _RowOp(f'{self} / {other}')

    def __radd__(self, other):
        return _RowOp(f'({self} + {other})')

    def __rsub__(self, other):
        return _RowOp(f'({self} - {other})')

    def __rmul__(self, other):
        return _RowOp(f'{self} * {other}')

    def __rtruediv__(self, other):
        return _RowOp(f'{self} / {other}')


class _RowOp(ROW):
    def __repr__(self):
        return self.column


def _or(*args):
    """
    `or` logical operator for filters
    """
    return '(' + ' or '.join(args) + ')'


def _and(*args):
    """
    `and` logical operator for filters
    """
    return '(' + ' and '.join(args) + ')'
