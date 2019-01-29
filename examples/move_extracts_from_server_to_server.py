# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import time

# This example is for a specific use case:
# When the Originating Tableau Server can connect to the live data source to generate the new extract, but the
# Destination Tableau Server cannot. This particular pattern downloads the updated TDSX file from the Originating Server
# then pushes it to the Destination Server without saving credentials. With no credentials (and no Extract Refresh
# scheduled), the version on the Destination Server will remain static until another version is pushed

o_server = u'http://'
o_username = u''
o_password = u''
o_site_content_url = u''

logger = Logger(u'move.log')

d_server = u'http://'
d_username = u''
d_password = u''
d_site_content_url = u''

t = TableauRestApiConnection28(o_server, o_username, o_password, o_site_content_url)
t.signin()
t.enable_logging(logger)
downloaded_filename = u'File Name'
wb_name_on_server = u'WB Name on Server'
proj_name = u'Default'
t.download_workbook(u'WB Name on Server', downloaded_filename, proj_name_or_luid=proj_name)

d = TableauRestApiConnection28(d_server, d_username, d_password, d_site_content_url)
d.signin()
d.enable_logging(logger)
proj = d.query_project(u'Default')
d.publish_workbook(u'{}.twbx'.format(downloaded_filename), wb_name_on_server, proj, save_credentials=False, overwrite=True)