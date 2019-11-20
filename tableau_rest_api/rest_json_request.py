import xml.etree.ElementTree as ET
# from HTMLParser import HTMLParser
# from StringIO import StringIO
from io import BytesIO
import re
import math
import copy
import requests
import sys
import json
from typing import Union, Any, Optional, List, Dict

from tableau_tools.logging_methods import LoggingMethods
from tableau_tools.logger import Logger
from tableau_tools.tableau_exceptions import *

# NOTE
# JSON Requests are not implemented for anything besides GET requests at the moment
# There is a lot of code in here based on the RestXmlRequest class, which would need to be cleaned up to handle
# all of the request types.
# Handles all of the actual HTTP calling
class RestJsonRequest(LoggingMethods):
    def __init__(self, url: Optional[str] = None, token: Optional[str] = None, logger: Optional[Logger] = None,
                 ns_map_url: str='http://tableau.com/api',
                 verify_ssl_cert: bool = True):
        super(self.__class__, self).__init__()

        # requests Session created to minimize connections
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json', 'Accept': 'application/json'})

        self.__defined_response_types = ('xml', 'png', 'binary', 'json')
        self.__defined_http_verbs = ('post', 'get', 'put', 'delete')
        self.url = url
        self._json_request: Optional[Dict] = None
        self.token = token
        self.__raw_response = None
        self.__last_error = None
        self.__last_url_request = None
        self.__last_response_headers = None
        self.__json_object: Optional[Dict] = None
        self.ns_map = {'t': ns_map_url}
        ET.register_namespace('t', ns_map_url)
        self.logger = logger
        self.log('RestJsonRequest intialized')
        self.__publish = None
        self.__boundary_string = None
        self.__publish_content = None
        self._http_verb = None
        self.__response_type = None
        self.__last_response_content_type = None
        self.__luid_pattern = r"[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*"
        self.__verify_ssl_cert = verify_ssl_cert

        try:
            self.http_verb = 'get'
            self.set_response_type('json')
        except:
            raise

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, token):
        self._token = token
        # Requests documentation says setting a dict value to None will remove it.
        self.session.headers.update({'X-tableau-auth': token, 'Content-Type': 'application/json',
                                     'Accept': 'application/json'})

    @property
    def json_request(self):
        return self._json_request

    @json_request.setter
    def json_request(self, json_request: Dict):
        self._json_request = json_request

    @property
    def http_verb(self):
        return self._http_verb

    @http_verb.setter
    def http_verb(self, verb):
        verb = verb.lower()
        if verb in self.__defined_http_verbs:
            self._http_verb = verb
        else:
            raise InvalidOptionException("HTTP Verb '{}' is not defined for this library".format(verb))

    def set_response_type(self, response_type):
        response_type = response_type.lower()
        if response_type in self.__defined_response_types:
            self.__response_type = response_type
        else:
            raise InvalidOptionException("Response type '{}' is not defined in this library".format(response_type))
        #if response_type == u'json':
            #self.session.headers.update({'Content-Type': 'application/json'})

    # Must set a boundary string when publishing
    def set_publish_content(self, content, boundary_string):
        if content is None and boundary_string is None:
            self.__publish = False
        else:
            self.__publish = True
        self.__boundary_string = boundary_string
        self.__publish_content = content

    def get_raw_response(self):
        return self.__raw_response

    def get_last_error(self):
        return self.__last_error

    def get_last_url_request(self):
        return self.__last_url_request

    def get_last_response_content_type(self):
        return self.__last_response_content_type

    def get_response(self) -> Union[Dict, bytes]:
        if self.__response_type == 'json' and self.__json_object is not None:
            self.log_debug("JSON Object Response: {}".format(json.dumps(self.__json_object)))
            return self.__json_object
        else:
            return self.__raw_response

    # Larger requests require pagination (starting at 1), thus page_number argument can be called.
    def __make_request(self, page_number=1):
        url = self.url
        if page_number > 0:
            param_separator = '?'
            # If already a parameter, just append
            if '?' in url:
                param_separator = '&'
            url += "{}pageNumber={}".format(param_separator, str(page_number))

        self.__last_url_request = url

        request_headers = {}

        if self.__publish is True:
            request_headers['Content-Type'] = 'multipart/mixed; boundary={}'.format(self.__boundary_string)

        # Need to handle binary return for image somehow
        self.log("Request {}  {}".format(self._http_verb.upper(), url))

        # Log the XML request being sent
        encoded_request = ""
        if self.json_request is not None:
            # Double check just in case someone sends through JSON as a string
            if (isinstance(self.json_request, str)):
                encoded_request = json.loads(self.json_request)
            else:
                encoded_request = self.json_request
            self.log("Request JSON: {}".format(self.json_request))
        if self.__publish_content is not None:
            encoded_request = self.__publish_content
        try:
            if self.http_verb == 'get':
                response = self.session.get(url, headers=request_headers, verify=self.__verify_ssl_cert)
            elif self.http_verb == 'delete':
                response = self.session.delete(url, headers=request_headers, verify=self.__verify_ssl_cert)
            elif self.http_verb == 'post':
                response = self.session.post(url, data=encoded_request, headers=request_headers,
                                             verify=self.__verify_ssl_cert)
            elif self.http_verb == 'put':
                response = self.session.put(url, data=encoded_request, headers=request_headers,
                                            verify=self.__verify_ssl_cert)
            else:
                raise InvalidOptionException('Must use one of the http verbs: get, post, put or delete')
            # To match previous exception handling pattern with urllib2
            response.raise_for_status()

            # Tableau 9.0 doesn't return real UTF-8 but escapes all unicode characters using numeric character encoding
            # initial_response = response.read()  # Leave the UTF8 decoding to later
            initial_response = response.content  # Leave the UTF8 decoding to later
            self.log('Response is of type {}'.format(type(initial_response)))

            # self.__last_response_content_type = response.info().getheader('Content-Type')
            self.__last_response_content_type = response.headers.get('Content-Type')

            self.log_debug("Content type from headers: {}".format(self.__last_response_content_type))

            # Don't bother with any extra work if the response is expected to be binary
            if self.__response_type == 'binary':
                self.__raw_response = initial_response
                return initial_response


            # Use HTMLParser to get rid of the escaped unicode sequences, then encode the thing as utf-8
            # parser = HTMLParser()
            # unicode_raw_response = parser.unescape(initial_response)
            unicode_raw_response = initial_response
            self._set_raw_response(unicode_raw_response)
            return True

        # Error detection
        except requests.exceptions.HTTPError as e:
            self._handle_http_error(e.response, e)

    def _handle_http_error(self, response, e):
        status_code = response.status_code
        # No recovering from a 500 (although this can happen for other reasons, possible worth expanding)
        if status_code >= 500:
            raise e
        # REST API returns 400 type errors that can be recovered from, so handle them
        raw_error_response = response.content
        self.log("Received a {} error, here was response:".format(str(status_code)))
        self.log(raw_error_response.decode('utf8'))

        json_obj = json.loads(raw_error_response.decode('utf8'))
        tableau_error = json_obj['error']
        error_code = tableau_error['code']
        detail_text = tableau_error['detail']
        detail_luid_match_obj = re.search(self.__luid_pattern, detail_text)
        if detail_luid_match_obj:
            detail_luid = detail_luid_match_obj.group(0)
        else:
            detail_luid = False
        self.log('Tableau REST API error code is: {}'.format(error_code))
        # If you are not signed in
        if error_code == '401000':
            raise NotSignedInException('401000 error, no session token was provided. Please sign in again.')
        if error_code == '401002':
            raise NotSignedInException(
                '401002 error, session token has timed out or otherwise been invalidated. Please sign in again.')

        # Everything that is not 400 can potentially be recovered from
        if status_code in [401, 402, 403, 404, 405, 409]:
            # If 'not exists' for a delete, recover and log
            if self._http_verb == 'delete':
                self.log('Delete action attempted on non-exists, keep going')
            if status_code == 409:
                self.log('HTTP 409 error, most likely an already exists')
            raise RecoverableHTTPException(status_code, error_code, detail_luid)
        else:
            raise e

    def _set_raw_response(self, unicode_raw_response):
        self.__raw_response = unicode_raw_response

    def request_from_api(self, page_number: Optional[int] = None) -> bool:

        if page_number is not None:
            self.__make_request(page_number)
            full_json_obj = json.loads(self.__raw_response)
            self.__json_object = full_json_obj
            self.log_debug("Logging the JSON object for page {}".format(page_number))
            self.log_debug(json.dumps(self.__json_object))
            self.log("Request succeeded")
            return True
        else:
            self.__make_request(1)
            if self.__response_type == 'json':
                if self.__raw_response == '' or self.__raw_response is None or len(self.__raw_response) == 0:
                    return True

                full_json_obj = json.loads(self.__raw_response)

                total_pages = 1
                for level_1 in full_json_obj:

                    if level_1 == 'pagination':

                        # page_number = int(pagination.get('pageNumber'))
                        page_size = int(full_json_obj['pagination']['pageSize'])
                        total_available = int(full_json_obj['pagination']['totalAvailable'])
                        total_pages = int(math.ceil(float(total_available) / float(page_size)))
                        self.log_debug('{} pages of content found'.format(total_pages))
                    else:
                        combined_json_obj = copy.deepcopy(full_json_obj[level_1])
                        if total_pages > 1:
                            self.log_debug('Working on the pages')
                            for i in range(2, total_pages + 1):
                                self.log_debug('Starting on page {}'.format(i))
                                self.__make_request(i)  # Get next page

                                full_json_obj = json.loads(self.__raw_response)
                                for l1 in full_json_obj:
                                    if l1 != 'pagination':
                                        for main_element in full_json_obj[l1]:
                                            # One level in to get to the list
                                            for list_element in full_json_obj[l1][main_element]:
                                                for e in combined_json_obj:
                                                    combined_json_obj[e].append(copy.deepcopy(list_element))

                        self.__json_object = combined_json_obj
                    self.log_debug("Logging the combined JSON object")
                    self.log_debug(json.dumps(self.__json_object))
                    self.log("Request succeeded")
                return True
            elif self.__response_type in ['binary', 'png', 'csv']:
                self.log('Non XML response')
                return True
