from .rest_api_base import *

# First Metrics Methods appear in API 3.9
class MetricsMethods39():
    def __init__(self, rest_api_base: TableauRestApiBase38):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

