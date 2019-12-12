import xml.etree.ElementTree as ET
# from HTMLParser import HTMLParser
# from StringIO import StringIO
from io import BytesIO
import re
import math
import copy
import requests
import sys
from typing import Union, Any, Optional, List, Dict, Tuple

from ..logging_methods import LoggingMethods
from ..tableau_exceptions import *
from ..logger import Logger

# Handles all of the actual HTTP calling
class RestXmlRequest(LoggingMethods):
    def __init__(self, url: Optional[str] = None, token: Optional[str] = None, logger: Optional[Logger] = None,
                 ns_map_url: str ='http://tableau.com/api',
                 verify_ssl_cert: bool = True):
        super(self.__class__, self).__init__()

        # requests Session created to minimize connections
        self.session = requests.Session()

        self.__defined_response_types = ('xml', 'png', 'binary', 'pdf')
        self.__defined_http_verbs = ('post', 'get', 'put', 'delete')
        self.url: str = url
        self._xml_request: Optional[ET.Element] = None
        self._token: str = token
        self.__raw_response = None
        self.__last_error = None
        self.__last_url_request = None
        self.__last_response_headers = None
        self.__xml_object = None
        self.__luid_pattern = r"[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*"

        # This sets the namespace globally so you can do XPath with t:
        self.ns_map = {'t': ns_map_url}
        ET.register_namespace('t', ns_map_url)

        self.logger = logger
        self.log('RestXmlRequest intialized')
        self.__publish = None
        self.__boundary_string = None
        self.__publish_content = None
        self._http_verb = None
        self.__response_type = None
        self.__last_response_content_type = None
        self.__verify_ssl_cert = verify_ssl_cert

        try:
            self.http_verb = 'get'
            self.set_response_type('xml')
        except:
            raise

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, token: str):
        self._token = token
        # Requests documentation says setting a dict value to None will remove it.
        self.session.headers.update({'X-tableau-auth': token})

    @property
    def xml_request(self) -> ET.Element:
        return self._xml_request

    @xml_request.setter
    def xml_request(self, xml_request: ET.Element):
        self._xml_request = xml_request

    @property
    def http_verb(self) -> str:
        return self._http_verb

    @http_verb.setter
    def http_verb(self, verb):
        verb = verb.lower()
        if verb in self.__defined_http_verbs:
            self._http_verb = verb
        else:
            raise InvalidOptionException("HTTP Verb '{}' is not defined for this library".format(verb))

    def set_response_type(self, response_type: str):
        response_type = response_type.lower()
        if response_type in self.__defined_response_types:
            self.__response_type = response_type
        else:
            raise InvalidOptionException("Response type '{}' is not defined in this library".format(response_type))

    # Must set a boundary string when publishing
    def set_publish_content(self, content: bytes, boundary_string: str):
        if content is None and boundary_string is None:
            self.__publish = False
        else:
            self.__publish = True
        self.__boundary_string = boundary_string
        self.__publish_content = content

    def get_raw_response(self) -> bytes:
        return self.__raw_response

    def get_last_error(self) -> str:
        return self.__last_error

    def get_last_url_request(self) -> str:
        return self.__last_url_request

    def get_last_response_content_type(self) -> str:
        return self.__last_response_content_type

    def get_response(self) -> Union[ET.Element, bytes]:
        if self.__response_type == 'xml' and self.__xml_object is not None:
            self.log_debug("XML Object Response: {}".format(ET.tostring(self.__xml_object, encoding='utf-8').decode('utf-8')))
            return self.__xml_object
        else:
            return self.__raw_response

    # Larger requests require pagination (starting at 1), thus page_number argument can be called.
    def __make_request(self, page_number:int = 1):
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

        # Log the XML request being sent
        encoded_request = ""
        if self.xml_request is None:
            self.log_uri(verb=self._http_verb.upper(), uri=url)
        if self.xml_request is not None:
            self.log_xml_request(xml=self.xml_request, verb=self.http_verb, uri=url)
            if isinstance(self.xml_request, str):
                encoded_request = self.xml_request.encode('utf-8')
            else:
                encoded_request = ET.tostring(self.xml_request, encoding='utf-8')
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
        self.log_error("{} error. Full response:".format(str(status_code)))
        self.log_error(raw_error_response.decode('utf8'))

        utf8_parser = ET.XMLParser(encoding='utf-8')
        xml = ET.parse(BytesIO(raw_error_response), parser=utf8_parser)
        try:
            tableau_error = xml.findall('.//t:error', namespaces=self.ns_map)
            error_code = tableau_error[0].get('code')
            tableau_detail = xml.findall('.//t:detail', namespaces=self.ns_map)
            detail_text = tableau_detail[0].text
        # This is to capture an error from the old API version when doing tests
        except IndexError:
            old_ns_map = {'t': 'http://tableausoftware.com/api'}
            tableau_error = xml.findall('.//t:error', namespaces=old_ns_map)
            error_code = tableau_error[0].get('code')
            tableau_detail = xml.findall('.//t:detail', namespaces=old_ns_map)
            detail_text = tableau_detail[0].text
        detail_luid_match_obj = re.search(self.__luid_pattern, detail_text)
        if detail_luid_match_obj:
            detail_luid = detail_luid_match_obj.group(0)
        else:
            detail_luid = False
        self.log_error('Tableau REST API error code: {}'.format(error_code))
        # If you are not signed in
        if error_code == '401000':
            raise NotSignedInException('401000 error, no session token was provided. Please sign in again.')
        if error_code == '401002':
            raise NotSignedInException('401002 error, session token has timed out or otherwise been invalidated. Please sign in again.')

        # Everything that is not 400 can potentially be recovered from
        if status_code in [401, 402, 403, 404, 405, 409]:
            # If 'not exists' for a delete, recover and log
            if self._http_verb == 'delete':
                self.log('Delete action attempted on non-exists, keep going')
            if status_code == 409:
                self.log('HTTP 409 error, most likely an already exists')
            raise RecoverableHTTPException(status_code, error_code, detail_luid)
        # Invalid Hyper Extract publish does this
        elif status_code == 400 and self._http_verb == 'post':
            if error_code == '400011':
                raise PossibleInvalidPublishException(http_code=400, tableau_error_code='400011',
                                                      msg="400011 on a Publish of a .hyper file could caused when the Hyper file either more than one table or the single table is not named 'Extract'.")
        else:
            raise e

    def _set_raw_response(self, unicode_raw_response):

        self.__raw_response = unicode_raw_response
        unicode_raw_response = unicode_raw_response.decode('utf-8')

        # Shows each individual request
        if self.__response_type == 'xml':
            self.log_xml_response(format(unicode_raw_response))

    # This has always brought back ALL listings from long paginated lists
    # But really should support three behaviors:
    # Single Page, All, and a "turbo search" mechanism for large lists of workbooks or data sources
    def request_from_api(self, page_number: int = 1):
        try:
            self.__make_request(page_number)
        except:
            raise
        if self.__response_type == 'xml':
            if self.__raw_response == '' or self.__raw_response is None or len(self.__raw_response) == 0:
                return True
            utf8_parser = ET.XMLParser(encoding='UTF-8')
            sio = BytesIO(self.__raw_response)
            xml = ET.parse(sio, parser=utf8_parser)
            # Set the XML object to the first returned. Will be replaced if there is pagination
            self.__xml_object = xml.getroot()

            for pagination in xml.findall('.//t:pagination', namespaces=self.ns_map):

                # page_number = int(pagination.get('pageNumber'))
                page_size = int(pagination.get('pageSize'))
                total_available = int(pagination.get('totalAvailable'))
                total_pages = int(math.ceil(float(total_available) / float(page_size)))

                full_xml_obj = None
                for obj in xml.getroot():
                    if obj.tag != 'pagination':
                        full_xml_obj = obj
                combined_xml_obj = copy.deepcopy(full_xml_obj)

                if total_pages > 1:
                    for i in range(2, total_pages + 1):

                        self.__make_request(i)  # Get next page
                        utf8_parser2 = ET.XMLParser(encoding='utf-8')
                        xml = ET.parse(BytesIO(self.__raw_response), parser=utf8_parser2)
                        for obj in xml.getroot():
                            if obj.tag != 'pagination':
                                full_xml_obj = obj
                        # This is the actual element, now need to append a copy to the big one
                        for e in full_xml_obj:
                            combined_xml_obj.append(e)

                self.__xml_object = combined_xml_obj
                self.log_xml_response("Combined XML Response")
                self.log_xml_response(ET.tostring(self.__xml_object, encoding='utf-8').decode('utf-8'))
                # self.log("Request succeeded")
                return True
        elif self.__response_type in ['binary', 'png', 'csv']:
            self.log('Non XML response')
            return True
