from .rest_api_base import *

# First Metadata Methods appear in API 3.5
class MetadataMethods35():
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)