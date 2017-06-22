from ..tableau_base import *
from ..tableau_exceptions import *
import urllib2
import xml.etree.ElementTree as etree
from HTMLParser import HTMLParser
from StringIO import StringIO
import re
import math
import copy
# import requests


# Handles all of the actual HTTP calling
class RestXmlRequest(TableauBase):
    def __init__(self, url, token=None, logger=None, ns_map_url='http://tableau.com/api'):
        """
        :param url:
        :param token:
        :param logger:
        :param ns_map_url:
        """
        super(self.__class__, self).__init__()
        self.__defined_response_types = (u'xml', u'png', u'binary')
        self.__defined_http_verbs = (u'post', u'get', u'put', u'delete')
        self.__base_url = url
        self.__xml_request = None
        self.__token = token
        self.__raw_response = None
        self.__last_error = None
        self.__last_url_request = None
        self.__last_response_headers = None
        self.__xml_object = None
        self.ns_map = {'t': ns_map_url}
        etree.register_namespace('t', ns_map_url)
        self.logger = logger
        self.__publish = None
        self.__boundary_string = None
        self.__publish_content = None
        self.__http_verb = None
        self.__response_type = None
        self.__last_response_content_type = None
        self.__luid_pattern = self.luid_pattern

        try:
            self.set_http_verb('get')
            self.set_response_type('xml')
        except:
            raise

    def set_xml_request(self, xml_request):
        """
        :type xml_request: Element
        :return: boolean
        """
        self.__xml_request = xml_request
        return True

    def set_http_verb(self, verb):
        verb = verb.lower()
        if verb in self.__defined_http_verbs:
            self.__http_verb = verb
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
            self.log(u"XML Object Response:\n {}".format(etree.tostring(self.__xml_object, encoding='utf8').decode('utf8')))
            return self.__xml_object
        else:
            return self.__raw_response

    # Internal method to handle all of the http request variations, using given library.
    # Using urllib2 with some modification, you could substitute in Requests or httplib
    # depending on preference. Must be able to do the verbs listed in self.defined_http_verbs
    # Larger requests require pagination (starting at 1), thus page_number argument can be called.
    def __make_request(self, page_number=1):
        self.log(u"HTTP verb is {}".format(self.__http_verb))
        url = self.__base_url.encode('utf8')
        if page_number > 0:
            param_separator = '?'
            # If already a parameter, just append
            if '?' in url:
                param_separator = '&'
            url += "{}pageNumber={}".format(param_separator, str(page_number))

        self.__last_url_request = url

        # Logic to create correct request
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(url)
        if self.__http_verb == u'delete':
            request.get_method = lambda: 'DELETE'

        if self.__http_verb == u'put' or self.__http_verb == u'post':
            if self.__publish_content is not None:
                request.add_data(self.__publish_content)
            elif self.__xml_request is not None:
                self.log(unicode((type(self.__xml_request))))
                if isinstance(self.__xml_request, str):
                    encoded_request = self.__xml_request.encode('utf8')
                else:
                    encoded_request = etree.tostring(self.__xml_request, encoding='utf8')
                request.add_data(encoded_request)
            else:
                request.add_data("")
        if self.__http_verb == u'put':
            request.get_method = lambda: 'PUT'
        if self.__token is not None:
            request.add_header('X-tableau-auth', self.__token.encode('utf8'))
        if self.__publish is True:
            request.add_header('Content-Type', 'multipart/mixed; boundary={}'.format(self.__boundary_string.encode('utf8')))

        # Need to handle binary return for image somehow
        try:
            self.log(u"Making REST request to Tableau Server using {}".format(self.__http_verb))
            self.log(u"Request URI: {}".format(url))
            if self.__xml_request is not None:
                self.log(u"Request XML:\n{}".format(self.__xml_request))
            response = opener.open(request)

            # Tableau 9.0 doesn't return real UTF-8 but escapes all unicode characters using numeric character encoding
            initial_response = response.read()  # Leave the UTF8 decoding to lxml
            self.__last_response_content_type = response.info().getheader('Content-Type')
            self.log(u"Content type from headers: {}".format(self.__last_response_content_type))
            # Don't botherw with any extra work if the response is expected to be binary
            if self.__response_type == u'binary':
                self.__raw_response = initial_response
                return initial_response

            # Use HTMLParser to get rid of the escaped unicode sequences, then encode the thing as utf-8
            parser = HTMLParser()
            unicode_raw_response = parser.unescape(initial_response)

            try:
                self.__raw_response = unicode_raw_response.encode('utf-8')
            # Sometimes it appears we actually send this stuff in UTF8
            except UnicodeDecodeError:
                self.__raw_response = unicode_raw_response
                unicode_raw_response = unicode_raw_response.decode('utf-8')

            if self.__response_type == 'xml':
                self.log(u"Raw Response:\n{}".format(unicode_raw_response))
            return True
        except urllib2.HTTPError as e:
            # No recoverying from a 500
            if e.code >= 500:
                raise
            # REST API returns 400 type errors that can be recovered from, so handle them
            raw_error_response = e.fp.read()
            self.log(u"Received a {} error, here was response:".format(unicode(e.code)))
            self.log(raw_error_response.decode('utf8'))

            utf8_parser = etree.XMLParser(encoding='utf-8')
            xml = etree.parse(StringIO(raw_error_response), parser=utf8_parser)
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
            # Everything that is not 400 can potentially be recovered from
            if e.code in [401, 402, 403, 404, 405, 409]:
                # If 'not exists' for a delete, recover and log
                if self.__http_verb == 'delete':
                    self.log(u'Delete action attempted on non-exists, keep going')
                if e.code == 409:
                    self.log(u'HTTP 409 error, most likely an already exists')
                raise RecoverableHTTPException(e.code, error_code, detail_luid)
            raise
        except:
            raise

    def request_from_api(self, page_number=1):
        try:
            self.__make_request(page_number)
        except:
            raise
        if self.__response_type == 'xml':
            if self.__raw_response == '':
                return True
            utf8_parser = etree.XMLParser(encoding='utf-8')
            xml = etree.parse(StringIO(self.__raw_response), parser=utf8_parser)
            # Set the XML object to the first returned. Will be replaced if there is pagination
            self.__xml_object = xml.getroot()
            combined_xml_obj = None

            for pagination in xml.findall(u'.//t:pagination', namespaces=self.ns_map):

                # page_number = int(pagination.get('pageNumber'))
                page_size = int(pagination.get('pageSize'))
                total_available = int(pagination.get('totalAvailable'))
                total_pages = int(math.ceil(float(total_available) / float(page_size)))

                full_xml_obj = None
                for obj in xml.getroot():
                    if obj.tag != 'pagination':
                        full_xml_obj = obj
                self.log("Full obj without paginatioin")
                self.log(etree.tostring(full_xml_obj))
                combined_xml_obj = copy.deepcopy(full_xml_obj)

                if total_pages > 1:
                    for i in xrange(2, total_pages + 1):

                        self.__make_request(i)  # Get next page
                        utf8_parser2 = etree.XMLParser(encoding='utf-8')
                        xml = etree.parse(StringIO(self.__raw_response), parser=utf8_parser2)
                        for obj in xml.getroot():
                            if obj.tag != 'pagination':
                                full_xml_obj = obj
                        # This is the actual element, now need to append a copy to the big one
                        for e in full_xml_obj:
                            combined_xml_obj.append(e)

                self.__xml_object = combined_xml_obj
                self.log(u"Logging the combined xml object")
                self.log(etree.tostring(self.__xml_object))
                return True
        elif self.__response_type in ['binary', 'png']:
            self.log(u'Binary response (binary or png) rather than XML')
            return True