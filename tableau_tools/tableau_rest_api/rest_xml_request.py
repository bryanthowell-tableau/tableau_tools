from ..tableau_base import *
from ..tableau_exceptions import *
import xml.etree.cElementTree as etree
# from HTMLParser import HTMLParser
# from StringIO import StringIO
from io import BytesIO
import re
import math
import copy
import requests
import sys


# Handles all of the actual HTTP calling
class RestXmlRequest(TableauBase):
    def __init__(self, url=None, token=None, logger=None, ns_map_url='http://tableau.com/api',
                 verify_ssl_cert=True):
        """
        :param url:
        :param token:
        :param logger:
        :param ns_map_url:
        """
        super(self.__class__, self).__init__()

        # requests Session created to minimize connections
        self.session = requests.Session()

        self.__defined_response_types = (u'xml', u'png', u'binary')
        self.__defined_http_verbs = (u'post', u'get', u'put', u'delete')
        self.url = url
        self._xml_request = None
        self._token = token
        self.__raw_response = None
        self.__last_error = None
        self.__last_url_request = None
        self.__last_response_headers = None
        self.__xml_object = None
        self.ns_map = {'t': ns_map_url}
        etree.register_namespace('t', ns_map_url)
        self.logger = logger
        self.log(u'RestXmlRequest intialized')
        self.__publish = None
        self.__boundary_string = None
        self.__publish_content = None
        self._http_verb = None
        self.__response_type = None
        self.__last_response_content_type = None
        self.__luid_pattern = self.luid_pattern
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
    def token(self, token):
        self._token = token
        # Requests documentation says setting a dict value to None will remove it.
        self.session.headers.update({'X-tableau-auth': token})

    @property
    def xml_request(self):
        return self._xml_request

    @xml_request.setter
    def xml_request(self, xml_request):
        """
        :type xml_request: Element
        :return: boolean
        """
        self._xml_request = xml_request

    @property
    def http_verb(self):
        return self._http_verb

    @http_verb.setter
    def http_verb(self, verb):
        verb = verb.lower()
        if verb in self.__defined_http_verbs:
            self._http_verb = verb
        else:
            raise InvalidOptionException(u"HTTP Verb '{}' is not defined for this library".format(verb))

    def set_response_type(self, response_type):
        response_type = response_type.lower()
        if response_type in self.__defined_response_types:
            self.__response_type = response_type
        else:
            raise InvalidOptionException(u"Response type '{}' is not defined in this library".format(response_type))

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

    def get_response(self):
        if self.__response_type == 'xml' and self.__xml_object is not None:
            self.log_debug(u"XML Object Response: {}".format(etree.tostring(self.__xml_object, encoding='utf-8').decode('utf-8')))
            return self.__xml_object
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
        self.log(u"Request {}  {}".format(self._http_verb.upper(), url))

        # Log the XML request being sent
        encoded_request = u""
        if self.xml_request is not None:
            self.log(u"Request XML: {}".format(etree.tostring(self.xml_request, encoding='utf-8').decode('utf-8')))
            if isinstance(self.xml_request, str):
                encoded_request = self.xml_request.encode('utf-8')
            else:
                encoded_request = etree.tostring(self.xml_request, encoding='utf-8')
        if self.__publish_content is not None:
            encoded_request = self.__publish_content
        try:
            if self.http_verb == u'get':
                response = self.session.get(url, headers=request_headers, verify=self.__verify_ssl_cert)
            elif self.http_verb == u'delete':
                response = self.session.delete(url, headers=request_headers, verify=self.__verify_ssl_cert)
            elif self.http_verb == u'post':
                response = self.session.post(url, data=encoded_request, headers=request_headers,
                                             verify=self.__verify_ssl_cert)
            elif self.http_verb == u'put':
                response = self.session.put(url, data=encoded_request, headers=request_headers,
                                            verify=self.__verify_ssl_cert)
            else:
                raise InvalidOptionException(u'Must use one of the http verbs: get, post, put or delete')
            # To match previous exception handling pattern with urllib2
            response.raise_for_status()

            # Tableau 9.0 doesn't return real UTF-8 but escapes all unicode characters using numeric character encoding
            # initial_response = response.read()  # Leave the UTF8 decoding to lxml
            initial_response = response.content  # Leave the UTF8 decoding to lxml
            self.log(u'Response is of type {}'.format(type(initial_response)))

            # self.__last_response_content_type = response.info().getheader('Content-Type')
            self.__last_response_content_type = response.headers.get(u'Content-Type')

            self.log_debug(u"Content type from headers: {}".format(self.__last_response_content_type))

            # Don't bother with any extra work if the response is expected to be binary
            if self.__response_type == u'binary':
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
            self._handle_http_error(response, e)

    def _handle_http_error(self, response, e):
        status_code = response.status_code
        # No recovering from a 500 (although this can happen for other reasons, possible worth expanding)
        if status_code >= 500:
            raise e
        # REST API returns 400 type errors that can be recovered from, so handle them
        raw_error_response = response.content
        self.log(u"Received a {} error, here was response:".format(unicode(status_code)))
        self.log(raw_error_response.decode('utf8'))

        utf8_parser = etree.XMLParser(encoding='utf-8')
        xml = etree.parse(BytesIO(raw_error_response), parser=utf8_parser)
        try:
            tableau_error = xml.findall(u'.//t:error', namespaces=self.ns_map)
            error_code = tableau_error[0].get('code')
            tableau_detail = xml.findall(u'.//t:detail', namespaces=self.ns_map)
            detail_text = tableau_detail[0].text
        # This is to capture an error from the old API version when doing tests
        except IndexError:
            old_ns_map = {'t': 'http://tableausoftware.com/api'}
            tableau_error = xml.findall(u'.//t:error', namespaces=old_ns_map)
            error_code = tableau_error[0].get('code')
            tableau_detail = xml.findall(u'.//t:detail', namespaces=old_ns_map)
            detail_text = tableau_detail[0].text
        detail_luid_match_obj = re.search(self.__luid_pattern, detail_text)
        if detail_luid_match_obj:
            detail_luid = detail_luid_match_obj.group(0)
        else:
            detail_luid = False
        self.log(u'Tableau REST API error code is: {}'.format(error_code))
        # If you are not signed in
        if error_code == u'401000':
            raise NotSignedInException(u'You must sign in first')
        # Everything that is not 400 can potentially be recovered from
        if status_code in [401, 402, 403, 404, 405, 409]:
            # If 'not exists' for a delete, recover and log
            if self._http_verb == 'delete':
                self.log(u'Delete action attempted on non-exists, keep going')
            if status_code == 409:
                self.log(u'HTTP 409 error, most likely an already exists')
            raise RecoverableHTTPException(status_code, error_code, detail_luid)
        else:
            raise e

    def _set_raw_response(self, unicode_raw_response):
        if sys.version_info[0] < 3:
            try:
                self.__raw_response = unicode_raw_response.encode('utf-8')
            # Sometimes it appears we actually send this stuff in UTF8
            except UnicodeDecodeError:
                self.__raw_response = unicode_raw_response
                unicode_raw_response = unicode_raw_response.decode('utf-8')
        else:
            self.__raw_response = unicode_raw_response
            unicode_raw_response = unicode_raw_response.decode('utf-8')

        if self.__response_type == 'xml':
            self.log_debug(u"Raw Response: {}".format(unicode_raw_response))

    def request_from_api(self, page_number=1):
        try:
            self.__make_request(page_number)
        except:
            raise
        if self.__response_type == 'xml':
            if self.__raw_response == '' or self.__raw_response is None or len(self.__raw_response) == 0:
                return True
            utf8_parser = etree.XMLParser(encoding='UTF-8')
            sio = BytesIO(self.__raw_response)
            xml = etree.parse(sio, parser=utf8_parser)
            # Set the XML object to the first returned. Will be replaced if there is pagination
            self.__xml_object = xml.getroot()

            for pagination in xml.findall(u'.//t:pagination', namespaces=self.ns_map):

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
                    for i in xrange(2, total_pages + 1):

                        self.__make_request(i)  # Get next page
                        utf8_parser2 = etree.XMLParser(encoding='utf-8')
                        xml = etree.parse(BytesIO(self.__raw_response), parser=utf8_parser2)
                        for obj in xml.getroot():
                            if obj.tag != 'pagination':
                                full_xml_obj = obj
                        # This is the actual element, now need to append a copy to the big one
                        for e in full_xml_obj:
                            combined_xml_obj.append(e)

                self.__xml_object = combined_xml_obj
                self.log_debug(u"Logging the combined xml object")
                self.log_debug(etree.tostring(self.__xml_object, encoding='utf-8').decode('utf-8'))
                self.log(u"Request succeeded")
                return True
        elif self.__response_type in ['binary', 'png', 'csv']:
            self.log(u'Non XML response')
            return True
