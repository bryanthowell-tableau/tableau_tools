from .rest_api_base import *

class ServerMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)



class ServerMethods27(ServerMethods):
    pass

class ServerMethods28(ServerMethods27):
    pass

class ServerMethods30(ServerMethods28):
    pass

class ServerMethods31(ServerMethods30):
    pass

class ServerMethods32(ServerMethods31):
    pass

class ServerMethods33(ServerMethods32):
    pass

class ServerMethods34(ServerMethods33):
    pass