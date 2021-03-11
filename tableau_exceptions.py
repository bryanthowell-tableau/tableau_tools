# -*- coding: utf-8 -*-

class TableauException(Exception):
    def __str__(self):
        return "{} Exception: {}".format(self.__class__.__name__,self.msg)

class NoMatchFoundException(TableauException):
    def __init__(self, msg):
        self.msg = msg


class AlreadyExistsException(TableauException):
    def __init__(self, msg, existing_luid):
        self.msg = msg
        self.existing_luid = existing_luid


# Raised when an action is attempted that requires being signed into that site
class NotSignedInException(TableauException):
    def __init__(self, msg):
        self.msg = msg


# Raise when something an option is passed that is not valid in the REST API (site_role, permissions name, etc)
class InvalidOptionException(TableauException):
    def __init__(self, msg):
        self.msg = msg


class RecoverableHTTPException(TableauException):
    def __init__(self, http_code, tableau_error_code, luid):
        self.http_code = http_code
        self.tableau_error_code = tableau_error_code
        self.luid = luid

class PossibleInvalidPublishException(TableauException):
    def __init__(self, http_code, tableau_error_code, msg):
        self.http_code = http_code
        self.tableau_error_code = tableau_error_code
        self.msg = msg

class MultipleMatchesFoundException(TableauException):
    def __init__(self, count):
        self.msg = 'Found {} matches for the request, something has the same name'.format(str(count))


class NoResultsException(TableauException):
    def __init__(self, msg):
        self.msg = msg
