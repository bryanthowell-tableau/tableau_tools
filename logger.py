import time
import sys
import xml.etree.ElementTree as ET
from typing import Union, Any, Optional, List, Dict, Tuple

# Logger has several modes
# Default just shows REST URL requests
# If you "enable_request_xml_logging", then it will show the full XML of the request
# If you "enable_debugging mode", then the log will indent to show which calls are wrapped within another
# "enabled_response_logging" will log the response
class Logger(object):
    def __init__(self, filename):
        self._log_level = 'standard'
        self.log_depth = 0
        try:
            lh = open(filename, 'wb')
            self.__log_handle = lh
        except IOError:
            print("Error: File '{}' cannot be opened to write for logging".format(filename))
            raise
        self._log_modes = {'debug': False, 'response': False, 'request': False}

    def enable_debug_level(self):
        self._log_level = 'debug'
        self._log_modes['debug'] = True

    def enable_request_logging(self):
        self._log_modes['request'] = True

    def enable_response_logging(self):
        self._log_modes['response'] = True

    def log(self, l: str):
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        if self.log_depth == 0:
            log_line = cur_time + " : " + l + "\n"
        else:
            log_line = " "*self.log_depth + "   " + cur_time + " : " + l + "\n"
        try:
            self.__log_handle.write(log_line.encode('utf8'))
        except UnicodeDecodeError as e:
            self.__log_handle.write(log_line)

    def log_debug(self, l: str):
        if self._log_modes['debug'] is True:
            self.log(l)

    def start_log_block(self):
        caller_function_name = sys._getframe(2).f_code.co_name
        c = str(sys._getframe(2).f_locals["self"].__class__)
        class_path = c.split('.')
        short_class = class_path[len(class_path)-1]
        short_class = short_class[:-2]
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


        # Only move the log depth in debug mode
        if self._log_modes['debug'] is True:
            self.log_depth += 2
            log_line = '{}vv-- {} : {} {} started --------vv\n'.format(" " * self.log_depth, str(cur_time), short_class,
                                                                       caller_function_name)
        else:
            log_line = '{} : {} {} started --------vv\n'.format(str(cur_time), short_class, caller_function_name)

        self.__log_handle.write(log_line.encode('utf-8'))

    def end_log_block(self):
        caller_function_name = sys._getframe(2).f_code.co_name
        c = str(sys._getframe(2).f_locals["self"].__class__)
        class_path = c.split('.')
        short_class = class_path[len(class_path)-1]
        short_class = short_class[:-2]
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        if self._log_modes['debug'] is True:
            self.log_depth -= 2
            log_line = '{}^^-- {} : {} {} ended --------^^\n'.format(" "*self.log_depth, str(cur_time), short_class, caller_function_name)
        else:
            log_line = '{} : {} {} ended --------^^\n'.format(str(cur_time), short_class, caller_function_name)

        self.__log_handle.write(log_line.encode('utf-8'))

    def log_uri(self, uri: str, verb: str):
        self.log('[{}] {}'.format(verb.upper(), uri))

    def log_xml_request(self, xml: Union[ET.Element, str], verb: str, uri: str):
        if self._log_modes['request'] is True:
            if isinstance(xml, str):
                self.log('[{}] \n{}'.format(verb.upper(), xml))
            else:
                self.log('[{}] \n{}'.format(verb.upper(), ET.tostring(xml)))
        else:
            self.log('[{}] {}'.format(verb.upper(), uri))

    def log_xml_response(self, xml: Union[str, ET.Element]):
        if self._log_modes['response'] is True:
            if isinstance(xml, str):
                self.log('[XML Response] \n{}'.format(xml))
            else:
                self.log('[XML Response] \n{}'.format(ET.tostring(xml)))

    def log_error(self, error_text: str):
        self.log('[ERROR] {}'.format(error_text))