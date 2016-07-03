import time
import sys


class Logger(object):
    def __init__(self, filename):
        try:
            lh = open(filename, 'wb')
            self.__log_handle = lh
        except IOError:
            print u"Error: File '{}' cannot be opened to write for logging".format(filename)
            raise

    def log(self, l):
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_line = cur_time + " : " + l + "\n"
        try:
            self.__log_handle.write(log_line.encode('utf8'))
        except UnicodeDecodeError as e:
            self.__log_handle.write(log_line)

    def start_log_block(self):
        caller_function_name = sys._getframe(2).f_code.co_name
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()).encode('utf-8')
        log_line = u'---------- {} started at {} ----------\n'.format(caller_function_name, cur_time)
        self.__log_handle.write(log_line.encode('utf-8'))

    def end_log_block(self):
        caller_function_name = sys._getframe(2).f_code.co_name
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()).encode('utf-8')
        log_line = u'---------- {} ended at {} ------------\n'.format(caller_function_name, cur_time)
        self.__log_handle.write(log_line.encode('utf-8'))

    def log_uri(self, uri, verb):
        self.log(u'Sending {} request via: \n{}'.format(verb, uri))

    def log_xml_request(self, xml, verb):
        self.log(u'Sending {} request with XML: \n{}'.format(verb, xml))