# -*- coding: utf-8 -*-

import zipfile
import os
import shutil
import codecs
from typing import Union, Any, Optional, List, Dict, Tuple
import xml.etree.ElementTree as ET
import io
from abc import ABC, abstractmethod

from tableau_tools.logging_methods import LoggingMethods
from tableau_tools.logger import Logger
from tableau_tools.tableau_exceptions import *
from .tableau_datasource import TableauDatasource
from .tableau_workbook import TableauWorkbook


# Hyper files are not considered in this situation as they are binary and generated a different way

# This is a helper class with factory and static methods
class TableauFileManager():

    @staticmethod
    def open(filename: str, logger_obj: Optional[Logger] = None):
        # logger_obj.log('Opening {}'.format(filename))
        # Packaged (X) files must come first because they are supersets
        if filename.lower().find('.tdsx') != -1:
            return TDSX(filename=filename, logger_obj=logger_obj)

        elif filename.lower().find('.twbx') != -1:
            return TWBX(filename=filename, logger_obj=logger_obj)

        elif filename.lower().find('.tflx') != -1:
            return TFLX(filename=filename, logger_obj=logger_obj)

        elif filename.lower().find('.twb') != -1:
            return TWB(filename=filename, logger_obj=logger_obj)

        elif filename.lower().find('.tds') != -1:
            return TDS(filename=filename, logger_obj=logger_obj)

        elif filename.lower().find('tfl') != -1:
            return TFL(filename=filename, logger_obj=logger_obj)

        else:
            raise InvalidOptionException('Must open a Tableau file with ending of tds, tdsx, twb, twbx, tfl, tflx')


    # For saving a TWB or TDS (or other) from a document object. Actually should be
    @staticmethod
    def create_new_tds(tableau_datasource: TableauDatasource):
        pass

    @staticmethod
    def create_new_tdsx(tableau_datasource: TableauDatasource):
        pass

    @staticmethod
    def create_new_twb(tableau_workbook: TableauWorkbook):
        pass

    @staticmethod
    def create_new_twbx(tableau_workbook: TableauWorkbook):
        pass


class DatasourceFileInterface(ABC):
    @property
    @abstractmethod
    def datasources(self) -> List[TableauDatasource]:
        #return self._datasources
        pass

# One of the principles of tableau_documents design is: Use the original XML as generated by Tableau Desktop
# as much as possible. Another principle: Wait to write anything until the save functions are called, so that in-memory changes
# are always included in their final state.
# In this model, the objects that end in File are responsible for reading and writing from disk, while the
# "tableau_document" objects handle any changes to XML.
# The saving chain is thus that a PackagedFile calls to a TableauFile to get the file it is writing
# and the TableauFile writes to disk by calling the TableauDocument to export its XML as a string

# Abstract implementation
class TableauXmlFile(LoggingMethods, ABC):
    def __init__(self, filename: str, logger_obj: Optional[Logger] = None):
        self.logger: Optional[Logger] = logger_obj
        self.tableau_document = None
        self.packaged_file: bool = False

    @property
    @abstractmethod
    def file_type(self) -> str:
        pass

    # Appropriate extension added if needed
    @abstractmethod
    def save_new_file(self, new_filename_no_extension: str) -> str:
        self.start_log_block()
        new_filename = new_filename_no_extension.split('.')[0]  # simple algorithm to kill extension
        if new_filename is None:
            new_filename = new_filename_no_extension
        self.log('Saving to a file with new filename {}'.format(new_filename))

        initial_save_filename = "{}.{}".format(new_filename_no_extension, self.file_type)
        # Make sure you don't overwrite the existing original file
        files = list(filter(os.path.isfile, os.listdir(os.curdir)))  # files only
        save_filename = initial_save_filename
        file_versions = 1
        while save_filename in files:
            name_parts = initial_save_filename.split(".")
            save_filename = "{} ({}).{}".format(name_parts[0], file_versions, name_parts[1])
            file_versions += 1

        self.tableau_document.save_file(save_filename)
        return save_filename

# At the moment, we don't do anything with the XML of the workbook except pull out the data sources and convert into TableauDatasource objects
# The reasoning is: workbooks are vastly bigger and more complex than data sources (just look at the file sizes sometimes)
# and opening that all in memory as an XML tree would be a disadvantage in most cases given that modifying
# data source attributes is the primary use case for this library
# That said, there should probably be a mechanism for work on aspects of the workbook that we feel are useful to modify
# and also possibly a full String Replace implemented for translation purposes
class TWB(DatasourceFileInterface, TableauXmlFile):
    def __init__(self, filename: str, logger_obj: Optional[Logger] = None):
        TableauXmlFile.__init__(self, filename=filename, logger_obj=logger_obj)
        self._open_and_initialize(filename=filename, logger_obj=logger_obj)

    def _open_and_initialize(self, filename, logger_obj):
        try:

            # The file needs to be opened as string so that String methods can be used to read each line
            wb_fh = codecs.open(filename, 'r', encoding='utf-8')
            # Rather than a temporary file, open up a file-like string object
            ds_fh = io.StringIO()

            # Stream through the file, only pulling the datasources section
            ds_flag = None
            # Previous versions threw out the metadata-records, but given the amount of RAM people have now,
            # and that Ask Data uses them in some for, there is no harm in leaving them now
            # Here we throw out metadata-records even when opening a workbook from disk, they take up space
            # and are recreate automatically.
            metadata_flag = None
            for line in wb_fh:
                # Grab the datasources

                #if line.find("<metadata-records") != -1 and metadata_flag is None:
                #    metadata_flag = True
                if ds_flag is True and metadata_flag is not True:
                    ds_fh.write(line)
                if line.find("<datasources") != -1 and ds_flag is None:
                    ds_flag = True
                    ds_fh.write("<datasources xmlns:user='http://www.tableausoftware.com/xml/user'>\n")
                #if line.find("</metadata-records") != -1 and metadata_flag is True:
                #    metadata_flag = False

                if line.find("</datasources>") != -1 and ds_flag is True:
                    break
            wb_fh.close()

            # File-like object has to be reset from the start for the next read
            ds_fh.seek(0)

            # Make ElementTree read it as XML (this may be overkill left over from Python2.7)
            utf8_parser = ET.XMLParser(encoding='utf-8')
            ds_xml = ET.parse(ds_fh, parser=utf8_parser)

            # Workbook is really a shell at this point, only handles compositing the XML back prior to save time
            self.tableau_document = TableauWorkbook(twb_filename=filename, logger_obj=logger_obj)
            # This generates the data source objects that live under the TableauWorkbook, including Parameters
            self.tableau_document.build_datasource_objects(datasource_xml=ds_xml)
            #file_obj.close()
            ds_fh.close()
        except IOError:
            self.log("Cannot open file {}".format(filename))
            raise

    @property
    def file_type(self) -> str:
        return 'twb'

    @property
    def datasources(self) -> List[TableauDatasource]:
        return self.tableau_document.datasources

    def save_new_file(self, filename_no_extension: str, save_to_directory: Optional[str] = None):
        self.start_log_block()
        file_extension = '.twb'

        try:
            # In case the .tds gets passed in from earlier
            filename_no_extension = filename_no_extension.split('.twb')[0]
            initial_save_filename = "{}{}".format(filename_no_extension, file_extension)
            # Make sure you don't overwrite the existing original file
            files = list(filter(os.path.isfile, os.listdir(os.curdir)))  # files only
            save_filename = initial_save_filename
            file_versions = 1
            while save_filename in files:
                name_parts = initial_save_filename.split(".")
                save_filename = "{} ({}).{}".format(name_parts[0], file_versions, name_parts[1])
                file_versions += 1

            if save_to_directory is not None:
                lh = codecs.open(save_to_directory + save_filename, 'w', encoding='utf-8')
            else:
                lh = codecs.open(save_filename, 'w', encoding='utf-8')

            # Write the XML header line
            #lh.write("<?xml version='1.0' encoding='utf-8' ?>\n\n")
            # Write the datasource XML itself
            wb_string = self.tableau_document.get_xml_string()
            if isinstance(wb_string, bytes):
                final_string = wb_string.decode('utf-8')
            else:
                final_string = wb_string
            lh.write(final_string)
            lh.close()

        except IOError:
            self.log("Error: File '{} cannot be opened to write to".format(filename_no_extension + file_extension))
            self.end_log_block()
            raise

class TDS(DatasourceFileInterface, TableauXmlFile):
    def __init__(self, filename: str, logger_obj: Optional[Logger] = None):
        TableauXmlFile.__init__(self, filename=filename, logger_obj=logger_obj)
        self._open_and_initialize(filename=filename, logger_obj=logger_obj)

    def _open_and_initialize(self, filename, logger_obj):
        try:

            # The file needs to be opened as string so that String methods can be used to read each line
            o_ds_fh = codecs.open(filename, 'r', encoding='utf-8')
            # Rather than a temporary file, open up a file-like string object
            ds_fh = io.StringIO()

            # Previous versions threw out the metadata-records, but given the amount of RAM people have now,
            # and that Ask Data uses them in some for, there is no harm in leaving them now
            # Here we throw out metadata-records even when opening a workbook from disk, they take up space
            # and are recreate automatically. Very similar to what we do in initialization of TableauWorkbook
            metadata_flag = None
            for line in o_ds_fh:
                # Grab the datasources
                #if line.find("<metadata-records") != -1 and metadata_flag is None:
                #    metadata_flag = True
                if metadata_flag is not True:
                    ds_fh.write(line)
                #if line.find("</metadata-records") != -1 and metadata_flag is True:
                #    metadata_flag = False
            o_ds_fh.close()
            # File-like object has to be reset from the start for the next read
            ds_fh.seek(0)

            # Make ElementTree read it as XML (this may be overkill left over from Python2.7)
            utf8_parser = ET.XMLParser(encoding='utf-8')
            ds_xml = ET.parse(ds_fh, parser=utf8_parser)

            self.tableau_document = TableauDatasource(datasource_xml=ds_xml.getroot(), logger_obj=logger_obj)
            ds_fh.close()
        except IOError:
            self.log("Cannot open file {}".format(filename))

    @property
    def file_type(self) -> str:
        return 'tds'

    @property
    def datasources(self) -> List[TableauDatasource]:
        return [self.tableau_document, ]

    def save_new_file(self, filename_no_extension: str, save_to_directory: Optional[str] = None):
        self.start_log_block()
        file_extension = '.tds'

        try:

            # In case the .tds gets passed in from earlier
            filename_no_extension = filename_no_extension.split('.tds')[0]
            initial_save_filename = "{}{}".format(filename_no_extension, file_extension)
            # Make sure you don't overwrite the existing original file
            files = list(filter(os.path.isfile, os.listdir(os.curdir)))  # files only
            save_filename = initial_save_filename
            file_versions = 1
            while save_filename in files:
                name_parts = initial_save_filename.split(".")
                save_filename = "{} ({}).{}".format(name_parts[0], file_versions, name_parts[1])
                file_versions += 1

            if save_to_directory is not None:
                lh = codecs.open(save_to_directory + save_filename, 'w', encoding='utf-8')
            else:
                lh = codecs.open(save_filename, 'w', encoding='utf-8')

            # Write the XML header line
            lh.write("<?xml version='1.0' encoding='utf-8' ?>\n\n")
            # Write the datasource XML itself
            ds_string = self.tableau_document.get_xml_string()
            if isinstance(ds_string, bytes):
                final_string = ds_string.decode('utf-8')
            else:
                final_string = ds_string
            lh.write(final_string)
            lh.close()

        except IOError:
            self.log("Error: File '{} cannot be opened to write to".format(filename_no_extension + file_extension))
            self.end_log_block()
            raise

# Abstract implementation
class TableauPackagedFile(LoggingMethods, ABC):
    def __init__(self, filename: str, logger_obj: Optional[Logger] = None):
        self.logger: Optional[Logger] = logger_obj
        self.log('TableauFile initializing for {}'.format(filename))
        self.packaged_file: bool = True
        self.packaged_filename: Optional[str] = None
        self.tableau_xml_file: TableauXmlFile

        self._original_file_type: Optional[str] = None

        self.other_files: List[str] = []
        self.temp_filename: Optional[str] = None
        self.orig_filename: str = filename
        self._document_type = None

        # Internal storage for use with swapping in new files from disk at save time
        self.file_replacement_map: Dict = {}

        # Packaged up nicely but always run in constructor
        self._open_file_and_initialize(filename=filename)

    @abstractmethod
    def _open_file_and_initialize(self, filename):
        pass

    @property
    def tableau_document(self) -> Union[TableauDatasource, TableauWorkbook]:
        return self.tableau_xml_file.tableau_document

    @property
    @abstractmethod
    def file_type(self) -> str:
        return self._original_file_type

    # This would be useful for interrogating Hyper files named within (should just be 1 per TDSX)
    def get_filenames_in_package(self):
        return self.other_files

    # If you know a file exists in the package, you can set it for replacement during the next save
    def set_file_for_replacement(self, filename_in_package: str, replacement_filname_on_disk: str):
        # No check for file, for later if building from scratch is allowed
        self.file_replacement_map[filename_in_package] = replacement_filname_on_disk

    # Appropriate extension added if needed
    def save_new_file(self, new_filename_no_extension: str) -> str:
        pass


class TDSX(DatasourceFileInterface, TableauPackagedFile):

    def _open_file_and_initialize(self, filename):
        try:
            file_obj = open(filename, 'rb')
            self.log('File type is {}'.format(self.file_type))
            # Extract the TWB or TDS file to disk, then create a sub TableauFile

            self.zf = zipfile.ZipFile(file_obj)
            # Ignore anything in the subdirectories
            for name in self.zf.namelist():
                if name.find('/') == -1:
                    if name.endswith('.tds'):
                        self.log('Detected a TDS file in archive, saving temporary file')
                        self.packaged_filename = os.path.basename(self.zf.extract(name))
                else:
                    self.other_files.append(name)

            self.tableau_xml_file = TDS(self.packaged_filename, self.logger)
            self._tableau_document = self.tableau_xml_file.tableau_document

            self.xml_name = None
            file_obj.close()
            # Clean up the file that was opened
            os.remove(self.packaged_filename)
        except IOError:
            self.log("Cannot open file {}".format(filename))
            raise

    @property
    def datasources(self) -> List[TableauDatasource]:
        return [self.tableau_document, ]

    @property
    def file_type(self) -> str:
        return 'tdsx'

    @property
    def tableau_document(self) -> TableauDatasource:
        return self.tableau_xml_file.tableau_document

    def save_new_file(self, new_filename_no_extension: str):
        self.start_log_block()
        new_filename = new_filename_no_extension.split('.')[0]  # simple algorithm to kill extension
        if new_filename is None:
            new_filename = new_filename_no_extension
        self.log('Saving to a file with new filename {}'.format(new_filename))

        initial_save_filename = "{}.{}".format(new_filename, 'tdsx')
        # Make sure you don't overwrite the existing original file
        files = list(filter(os.path.isfile, os.listdir(os.curdir)))  # files only
        save_filename = initial_save_filename
        file_versions = 1
        while save_filename in files:
            name_parts = initial_save_filename.split(".")
            save_filename = "{} ({}).{}".format(name_parts[0],file_versions, name_parts[1])
            file_versions += 1
        new_zf = zipfile.ZipFile(save_filename, 'w', zipfile.ZIP_DEFLATED)

        # Call to the TableauXmlFile object to write the file to disk
        self.tableau_xml_file.save_new_file(filename_no_extension=self.packaged_filename)
        new_zf.write(self.packaged_filename)
        self.log('Removing file {}'.format(self.packaged_filename))
        os.remove(self.packaged_filename)

        temp_directories_to_remove = {}
        # Store any directories that already existed, so you don't clean up exiting directory on disk
        existing_directories = []
        all_in_working_dir = os.listdir()
        for l in all_in_working_dir:
            if (os.path.isdir(l)):
                existing_directories.append(l)
        # Now actually go through all the other files, extract them, put into the new ZIP file, then clean up
        if len(self.other_files) > 0:
            file_obj = open(self.orig_filename, 'rb')
            o_zf = zipfile.ZipFile(file_obj)

            for filename in self.other_files:
                self.log('Looking into additional files: {}'.format(filename))
                lowest_level = filename.split('/')
                # Only delete out if it didn't exist prior to saving out of the original ZIPfile
                if lowest_level[0] not in existing_directories:
                    temp_directories_to_remove[lowest_level[0]] = True
                if self.file_replacement_map and filename in self.file_replacement_map:
                    new_zf.write(self.file_replacement_map[filename], "/" + filename)
                    # Delete from the data_file_replacement_map to reduce down to end
                    del self.file_replacement_map[filename]
                else:
                    o_zf.extract(filename)
                    new_zf.write(filename)
                    # sometimes a lonely dir is in the zip file
                    if os.path.isdir(filename):
                        os.rmdir(filename)
                    else:
                        os.remove(filename)
                self.log('Removed file {}'.format(filename))

            file_obj.close()

        # Loop through remaining files in data_file_replacement_map to just add
        for filename in self.file_replacement_map:
            new_zf.write(self.file_replacement_map[filename], "/" + filename)

        # Cleanup all the temporary directories
        for directory in temp_directories_to_remove:
            self.log('Removing directory {}'.format(directory))
            try:
                shutil.rmtree(directory)
            except OSError as e:
                # Just means that directory didn't exist for some reason, probably a swap occurred
                pass
        new_zf.close()

        return save_filename


class TWBX(DatasourceFileInterface, TableauPackagedFile):

    #self._open_file_and_intialize(filename=filename)

    def _open_file_and_initialize(self, filename):
        try:
            file_obj = open(filename, 'rb')
            self.log('File type is {}'.format(self.file_type))
            # Extract the TWB or TDS file to disk, then create a sub TableauFile

            self.zf = zipfile.ZipFile(file_obj)
            # Ignore anything in the subdirectories
            for name in self.zf.namelist():
                if name.find('/') == -1:
                    if name.endswith('.twb'):
                        self.log('Detected a TWB file in archive, saving temporary file')
                        self.packaged_filename = os.path.basename(self.zf.extract(name))
                else:
                    self.other_files.append(name)

            self.tableau_xml_file = TWB(self.packaged_filename, self.logger)
            # self._tableau_document = self.tableau_xml_file._tableau_document

            file_obj.close()
            # Clean up the file that was opened
            os.remove(self.packaged_filename)
        except IOError:
            self.log("Cannot open file {}".format(filename))
            raise

    @property
    def datasources(self) -> List[TableauDatasource]:
        return self.tableau_document.datasources

    @property
    def tableau_document(self) -> TableauWorkbook:
        return self.tableau_xml_file.tableau_document


    @property
    def file_type(self) -> str:
        return 'twbx'

    # Make sure to open save the original TWB file to disk PRIOR to asking for the XML from TableauWorkbook, because it
    # won't be able to find it via that filename if it doesn't exist!
    # Appropriate extension added if needed
    def save_new_file(self, new_filename_no_extension: str):
        self.start_log_block()
        new_filename = new_filename_no_extension.split('.')[0]  # simple algorithm to kill extension
        if new_filename is None:
            new_filename = new_filename_no_extension
        self.log('Saving to a file with new filename {}'.format(new_filename))

        initial_save_filename = "{}.{}".format(new_filename, 'twbx')
        # Make sure you don't overwrite the existing original file
        files = list(filter(os.path.isfile, os.listdir(os.curdir)))  # files only
        save_filename = initial_save_filename
        file_versions = 1
        while save_filename in files:
            name_parts = initial_save_filename.split(".")
            save_filename = "{} ({}).{}".format(name_parts[0],file_versions, name_parts[1])
            file_versions += 1
        new_zf = zipfile.ZipFile(save_filename, 'w', zipfile.ZIP_DEFLATED)
        # Save the object down
        self.log('Creating temporary XML file {}'.format(self.packaged_filename))
        # Have to extract the original TWB to temporary file, with different name, then prep the TableauWorkbook object
        # with that filename so it can insert in any changed datasources
        self.log('Creating from original file {}'.format(self.orig_filename))
        file_obj = open(self.orig_filename, 'rb')
        temp_wb_filename = 'temp.twb'
        o_zf = zipfile.ZipFile(file_obj)
        o_zf.extract(self.tableau_document.twb_filename)
        shutil.copy(self.tableau_document.twb_filename, temp_wb_filename)
        os.remove(self.tableau_document.twb_filename)
        self.tableau_document.twb_filename = temp_wb_filename
        file_obj.close()

        # Call to the TableauXmlFile object to write the file to disk
        self.tableau_xml_file.save_new_file(filename_no_extension=self.packaged_filename)
        new_zf.write(self.packaged_filename)

        # Clean up the new file and the temp file
        os.remove(self.packaged_filename)
        os.remove(temp_wb_filename)

        temp_directories_to_remove = {}

        if len(self.other_files) > 0:
            file_obj = open(self.orig_filename, 'rb')
            o_zf = zipfile.ZipFile(file_obj)

            for filename in self.other_files:
                self.log('Looking into additional files: {}'.format(filename))

                if self.file_replacement_map and filename in self.file_replacement_map:
                    new_zf.write(self.file_replacement_map[filename], "/" + filename)
                    # Delete from the data_file_replacement_map to reduce down to end
                    del self.file_replacement_map[filename]
                else:
                    o_zf.extract(filename)
                    new_zf.write(filename)
                    # sometimes a lonely dir is in the zip file
                    if os.path.isdir(filename):
                        os.rmdir(filename)
                    else:
                        os.remove(filename)
                self.log('Removed file {}'.format(filename))
                lowest_level = filename.split('/')
                temp_directories_to_remove[lowest_level[0]] = True
            file_obj.close()

        # Loop through remaining files in data_file_replacement_map to just add
        for filename in self.file_replacement_map:
            new_zf.write(self.file_replacement_map[filename], "/" + filename)

        # Cleanup all the temporary directories
        for directory in temp_directories_to_remove:
            self.log('Removing directory {}'.format(directory))
            try:
                shutil.rmtree(directory)
            except OSError as e:
                # Just means that directory didn't exist for some reason, probably a swap occurred
                pass
        new_zf.close()

        return save_filename

# TFL files are actually JSON rather than XML. I don't think there are actually any XML calls except
# possibly when creating the TableauDocument file.
class TFL(TableauXmlFile):

    @property
    def file_type(self) -> str:
        return 'tfl'

class TFLX(TableauPackagedFile):

    @property
    def file_type(self) -> str:
        return 'tflx'