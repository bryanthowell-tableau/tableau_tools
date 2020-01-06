from typing import Optional, List, Dict, Union
import xml.etree.ElementTree as ET

from .logger import Logger

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
            self.logger.log_uri(uri=uri, verb=verb)

    def log_xml_request(self, xml: ET.Element, verb: str, uri: str):
        if self.logger is not None:
            self.logger.log_xml_request(xml=xml, verb=verb, uri=uri)

    def log_xml_response(self, xml: Union[str, ET.Element]):
        if self.logger is not None:
            self.logger.log_xml_response(xml=xml)

    def log_error(self, error_text: str):
        if self.logger is not None:
            self.logger.log_error(error_text=error_text)