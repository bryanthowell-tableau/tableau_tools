# -*- coding: utf-8 -*-


class NoMatchFoundException(Exception):
    def __init__(self, msg):
        self.msg = msg


class AlreadyExistsException(Exception):
    def __init__(self, msg, existing_luid):
        self.msg = msg
        self.existing_luid = existing_luid


# Raised when an action is attempted that requires being signed into that site
class NotSignedInException(Exception):
    def __init__(self, msg):
        self.msg = msg


# Raise when something an option is passed that is not valid in the REST API (site_role, permissions name, etc)
class InvalidOptionException(Exception):
    def __init__(self, msg):
        self.msg = msg


class RecoverableHTTPException(Exception):
    def __init__(self, http_code, tableau_error_code, luid):
        self.http_code = http_code
        self.tableau_error_code = tableau_error_code
        self.luid = luid


class MultipleMatchesFoundException(Exception):
    def __init__(self, count):
        self.msg = u'Found {} matches for the request, something has the same name'.format(unicode(count))


class NoResultsException(Exception):
    def __init__(self, msg):
        self.msg = msg
