from typing import Optional, List, Dict
import xml.etree.ElementTree as ET

from tableau_tools.logger import Logger

class LoggingMethods:
    # Logging Methods
    def enable_logging(self, logger_obj: Logger):
        self.logger = logger_obj

    def log(self, l: str):
        if self.logger is not None:
            self.logger.log(l)

    def log_debug(self, l: str):
        if self.logger is not None:
            self.logger.log_debug(l)

    def start_log_block(self):
        if self.logger is not None:
            self.logger.start_log_block()

    def end_log_block(self):
        if self.logger is not None:
            self.logger.end_log_block()

    def log_uri(self, uri: str, verb: str):
        if self.logger is not None:
            self.logger.log_uri(verb, uri)

    def log_xml_request(self, xml: ET.Element, verb: str):
        if self.logger is not None:
            self.logger.log_xml_request(verb, xml)