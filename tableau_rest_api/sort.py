from ..tableau_exceptions import *


class Sort:
    def __init__(self, field: str, direction: str):
        self.field = field
        if direction not in ['asc', 'desc']:
            raise InvalidOptionException('Sort direction must be asc or desc')
        self.direction = direction

    def get_sort_string(self) -> str:
        sort_string = '{}:{}'.format(self.field, self.direction)
        return sort_string

class SortAscending(Sort):
    def __init__(self, field: str):
        Sort.__init__(self, field=field, direction='asc')


class SortDescending(Sort):
    def __init__(self, field: str):
        Sort.__init__(self, field=field, direction='desc')

