import time
import sys
import xml.etree.ElementTree as etree


class Logger(object):
    def __init__(self, filename):
        self._log_level = u'standard'
        self.log_depth = 0
        try:
            lh = open(filename, 'wb')
            self.__log_handle = lh
        except IOError:
            print u"Error: File '{}' cannot be opened to write for logging".format(filename)
            raise

    def enable_debug_level(self):
        self._log_level = u'debug'

    def log(self, l):
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        if self.log_depth == 0:
            log_line = cur_time + " : " + l + "\n"
        else:
            log_line = " "*self.log_depth + "   " + cur_time + " : " + l + "\n"
        try:
            self.__log_handle.write(log_line.encode('utf8'))
        except UnicodeDecodeError as e:
            self.__log_handle.write(log_line)

    def log_debug(self, l):
        if self._log_level == u'debug':
            self.log(l)

    def start_log_block(self):
        caller_function_name = sys._getframe(2).f_code.co_name
        c = unicode(sys._getframe(2).f_locals["self"].__class__)
        class_path = c.split('.')
        short_class = class_path[len(class_path)-1]
        short_class = short_class[:-2]
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()).encode('utf-8')

        log_line = u'{}vv-- {} : {} {} started --------vv\n'.format(u" "*self.log_depth, cur_time, short_class, caller_function_name)
        self.log_depth += 2
        self.__log_handle.write(log_line.encode('utf-8'))

    def end_log_block(self):
        caller_function_name = sys._getframe(2).f_code.co_name
        c = unicode(sys._getframe(2).f_locals["self"].__class__)
        class_path = c.split('.')
        short_class = class_path[len(class_path)-1]
        short_class = short_class[:-2]
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()).encode('utf-8')
        self.log_depth -= 2
        log_line = u'{}^^-- {} : {} {} ended --------^^\n'.format(u" "*self.log_depth, cur_time, short_class, caller_function_name)

        self.__log_handle.write(log_line.encode('utf-8'))

    def log_uri(self, uri, verb):
        self.log(u'Sending {} request via: \n{}'.format(verb, uri))

    def log_xml_request(self, xml, verb):
        if isinstance(xml, basestring):
            self.log(u'Sending {} request with XML: \n{}'.format(verb, xml))
        else:
            self.log(u'Sending {} request with XML: \n{}'.format(verb, etree.tostring(xml)))
