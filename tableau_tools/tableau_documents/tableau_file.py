# -*- coding: utf-8 -*-

import zipfile
import os
import shutil
from ..tableau_base import *
from tableau_datasource import TableauDatasource
from tableau_workbook import TableauWorkbook
from tableau_document import TableauDocument
import codecs


class TableauFile(TableauBase):

    def __init__(self, filename, logger_obj=None, create_new=False, ds_version=u'10'):
        """
        :type filename: unicode
        :type logger_obj: Logger
        """
        self.logger = logger_obj
        self.log(u'TableauFile initializing for {}'.format(filename))
        self.packaged_file = False
        self.packaged_filename = None
        self.tableau_xml_file = None
        self._tableau_document = None
        self._original_file_type = None
        self._final_file_type = None
        self.other_files = []
        self.temp_filename = None
        self.orig_filename = filename
        if filename is None:
            # Assume we start as TDS when building from scratch
            self._original_file_type = u'tds'
            self._final_file_type = u'tds'
        if filename.lower().find(u'.tdsx') != -1:
            self._original_file_type = u'tdsx'
            self._final_file_type = u'tdsx'
            self.packaged_file = True
        elif filename.lower().find(u'.twbx') != -1:
            self._original_file_type = u'twbx'
            self._final_file_type = u'twbx'
            self.packaged_file = True
        elif filename.lower().find(u'.twb') != -1:
            self._original_file_type = u'twb'
            self._final_file_type = u'twb'
        elif filename.lower().find(u'.tds') != -1:
            self._original_file_type = u'tds'
            self._final_file_type = u'tds'
        else:
            raise InvalidOptionException(u'Must open a Tableau file with ending of tds, tdsx, twb, or twbx')
        try:
            if create_new is True:
                if self._original_file_type in [u'tds', u'tdsx']:
                    self._tableau_document = TableauDatasource(None, logger_obj, ds_version=ds_version)
                else:
                    raise InvalidOptionException(u'Cannot create a new TWB or TWBX from scratch currently')
            else:
                file_obj = open(filename, 'rb')
                self.log(u'File type is {}'.format(self.file_type))
                # Extract the TWB or TDS file to disk, then create a sub TableauFile
                if self.file_type in [u'twbx', u'tdsx']:
                    self.zf = zipfile.ZipFile(file_obj)
                    # Ignore anything in the subdirectories
                    for name in self.zf.namelist():
                        if name.find('/') == -1:
                            if name.endswith('.tds'):
                                self.log(u'Detected a TDS file in archive, saving temporary file')
                                self.packaged_filename = os.path.basename(self.zf.extract(name))
                            elif name.endswith('.twb'):
                                self.log(u'Detected a TWB file in archive, saving temporary file')
                                self.packaged_filename = os.path.basename(self.zf.extract(name))
                        else:
                            self.other_files.append(name)

                    self.tableau_xml_file = TableauFile(self.packaged_filename, self.logger)
                    self._tableau_document = self.tableau_xml_file.tableau_document
                elif self.file_type == u'twb':
                    self._tableau_document = TableauWorkbook(filename, self.logger)
                elif self.file_type == u'tds':
                    # Here we throw out metadata-records even when opening a workbook from disk, they take up space
                    # and are recreate automatically. Very similar to what we do in initialization of TableauWorkbook
                    o_ds_fh = codecs.open(filename, 'r', encoding='utf-8')
                    ds_fh = codecs.open(u'temp_file.txt', 'w', encoding='utf-8')
                    self.temp_filename = u'temp_file.txt'
                    metadata_flag = None
                    for line in o_ds_fh:
                        # Grab the datasources

                        if line.find(u"<metadata-records") != -1 and metadata_flag is None:
                            metadata_flag = True
                        if metadata_flag is not True:
                            ds_fh.write(line)
                        if line.find(u"</metadata-records") != -1 and metadata_flag is True:
                            metadata_flag = False
                    o_ds_fh.close()

                    ds_fh.close()
                    utf8_parser = etree.XMLParser(encoding='utf-8')

                    ds_xml = etree.parse(u'temp_file.txt', parser=utf8_parser)

                    self._tableau_document = TableauDatasource(ds_xml.getroot(), self.logger)
                self.xml_name = None
                file_obj.close()
        except IOError:
            self.log(u"Cannot open file {}".format(filename))
            raise

    @property
    def file_type(self):
        return self._original_file_type

    @property
    def tableau_document(self):
        """
        :rtype: TableauDocument
        """
        return self._tableau_document

    # Appropriate extension added if needed
    def save_new_file(self, new_filename_no_extension, data_file_replacement_map=None):
        """
        :type new_filename_no_extension: unicode
        :type data_file_replacement_map: dict
        :rtype: unicode
        """
        self.start_log_block()
        new_filename = new_filename_no_extension.split('.')[0]  # simple algorithm to kill extension
        if new_filename is None:
            new_filename = new_filename_no_extension
        self.log(u'Saving to a file with new filename {}'.format(new_filename))
        # Change filetype if there are new extracts to add
        for ds in self.tableau_document.datasources:
            if ds.tde_filename is not None:
                if self.file_type == u'twb':
                    self._final_file_type = u'twbx'
                    self.packaged_filename = u"{}.twb".format(new_filename)
                    self.log(u'Final filetype will be TWBX')
                    break
                if self.file_type == u'tds':
                    self._final_file_type = u'tdsx'
                    self.packaged_filename = u"{}.tds".format(new_filename)
                    self.log(u'Final filetype will be TDSX')
                    break

        if self._final_file_type in [u'twbx', u'tdsx']:
            initial_save_filename = u"{}.{}".format(new_filename, self._final_file_type)
            # Make sure you don't overwrite the existing original file
            files = filter(os.path.isfile, os.listdir(os.curdir))  # files only
            save_filename = initial_save_filename
            file_versions = 1
            while save_filename in files:
                name_parts = initial_save_filename.split(u".")
                save_filename = u"{} ({}).{}".format(name_parts[0],file_versions, name_parts[1])
                file_versions += 1
            new_zf = zipfile.ZipFile(save_filename, 'w', zipfile.ZIP_DEFLATED)
            # Save the object down
            self.log(u'Creating temporary XML file {}'.format(self.packaged_filename))
            # Have to extract the original TWB to temporary file
            self.log(u'Creating from original file {}'.format(self.orig_filename))
            if self._original_file_type == u'twbx':
                file_obj = open(self.orig_filename, 'rb')
                o_zf = zipfile.ZipFile(file_obj)
                o_zf.extract(self.tableau_document.twb_filename)
                shutil.copy(self.tableau_document.twb_filename, u'temp.twb')
                os.remove(self.tableau_document.twb_filename)
                self.tableau_document.twb_filename = u'temp.twb'
                file_obj.close()
            self.tableau_document.save_file(self.packaged_filename)
            new_zf.write(self.packaged_filename)
            self.log(u'Removing file {}'.format(self.packaged_filename))
            os.remove(self.packaged_filename)

            if self._original_file_type == u'twbx':
                os.remove(u'temp.twb')
                self.log(u'Removed file temp.twb'.format(self.packaged_filename))

            temp_directories_to_remove = {}

            if len(self.other_files) > 0:
                file_obj = open(self.orig_filename, 'rb')
                o_zf = zipfile.ZipFile(file_obj)

                # Find datasources with new extracts, and skip their files
                extracts_to_skip = []
                for ds in self.tableau_document.datasources:
                    if ds.existing_tde_filename is not None and ds.tde_filename is not None:
                        extracts_to_skip.append(ds.existing_tde_filename)

                for filename in self.other_files:
                    self.log(u'Looking into additional files: {}'.format(filename))

                    # Skip extracts listed for replacement
                    if filename in extracts_to_skip:
                        self.log(u'File {} is from an extract that has been replaced, skipping'.format(filename))
                        continue

                    # If file is listed in the data_file_replacement_map, write data from the mapped in file
                    if data_file_replacement_map and filename in data_file_replacement_map:
                        #data_file_obj = open(filename, mode='wb')
                        #data_file_obj.write(data_file_replacement_map[filename])
                        #data_file_obj.close()
                        new_zf.write(data_file_replacement_map[filename], u"/" + filename)
                    else:
                        o_zf.extract(filename)
                        new_zf.write(filename)
                        os.remove(filename)
                    self.log(u'Removed file {}'.format(filename))
                    lowest_level = filename.split('/')
                    temp_directories_to_remove[lowest_level[0]] = True
                file_obj.close()
            # If new extract, write that file
            for ds in self.tableau_document.datasources:
                if ds.tde_filename is not None:
                    new_zf.write(ds.tde_filename, u'/Data/Datasources/{}'.format(ds.tde_filename))
                    os.remove(ds.tde_filename)
                    self.log(u'Removed file {}'.format(ds.tde_filename))

            # Cleanup all the temporary directories
            for directory in temp_directories_to_remove:
                self.log(u'Removing directory {}'.format(directory))
                try:
                    shutil.rmtree(directory)
                except OSError as e:
                    # Just means that directory didn't exist for some reason, probably a swap occurred
                    pass
            new_zf.close()

            return save_filename
        else:
            initial_save_filename = u"{}.{}".format(new_filename_no_extension, self.file_type)
            # Make sure you don't overwrite the existing original file
            files = filter(os.path.isfile, os.listdir(os.curdir))  # files only
            save_filename = initial_save_filename
            file_versions = 1
            while save_filename in files:
                name_parts = initial_save_filename.split(u".")
                save_filename = u"{} ({}).{}".format(name_parts[0],file_versions, name_parts[1])
                file_versions += 1

            self.tableau_document.save_file(save_filename)
            return save_filename
