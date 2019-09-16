from ..tableau_exceptions import *


class Sort:
    def __init__(self, field, direction):
        """
        :type field: unicode
        :param direction: must be asc or desc
        :type direction: uniode
        """
        self.field = field
        if direction not in ['asc', 'desc']:
            raise InvalidOptionException('Sort direction must be asc or desc')
        self.direction = direction

    def get_sort_string(self):
        """
        :rtype: unicode
        """
        sort_string = '{}:{}'.format(self.field, self.direction)
        return sort_string

