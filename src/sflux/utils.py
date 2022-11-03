class ROW:
    """"""
    def __init__(self, column: str):
        self.column = column

    def __eq__(self, other: str):
        return f'r["{self.column}"] == "{other}"'

    def __ne__(self, other):
        return f'r["{self.column}"] != "{other}"'

    def __ge__(self, other):
        return f'r["{self.column}"] >= "{other}"'

    def __gt__(self, other):
        return f'r["{self.column}"] > "{other}"'

    def __le__(self, other):
        return f'r["{self.column}"] <= "{other}"'

    def __lt__(self, other):
        return f'r["{self.column}"] < "{other}"'


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
