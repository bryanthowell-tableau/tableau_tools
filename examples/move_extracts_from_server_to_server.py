# -*- coding: utf-8 -*-

from tableau_tools import *
import time

# This example is for a specific use case:
# When the Originating Tableau Server can connect to the live data source to generate the new extract, but the
# Destination Tableau Server cannot. This particular pattern downloads the updated TDSX file from the Originating Server
# then pushes it to the Destination Server without saving credentials. With no credentials (and no Extract Refresh
# scheduled), the version on the Destination Server will remain static until another version is pushed

o_server = 'http://'
o_username = ''
o_password = ''
o_site_content_url = ''

logger = Logger('move.log')

d_server = 'http://'
d_username = ''
d_password = ''
d_site_content_url = ''


t = TableauServerRest28(server=o_server, username=o_username, password=o_password, site_content_url=o_site_content_url)
t.signin()
t.enable_logging(logger)
downloaded_filename = 'File Name'
wb_name_on_server = 'WB Name on Server'
proj_name = 'Default'
t.workbooks.download_workbook(wb_name_or_luid='WB Name on Server', filename_no_extension=downloaded_filename,
                              proj_name_or_luid=proj_name)

d = TableauServerRest28(d_server, d_username, d_password, d_site_content_url)
d.signin()
d.enable_logging(logger)
proj = d.projects.query_project('Default')
d.workbooks.publish_workbook(workbook_filename='{}.twbx'.format(downloaded_filename),
                             workbook_name=wb_name_on_server, project_obj=proj,
                             save_credentials=False, overwrite=True)