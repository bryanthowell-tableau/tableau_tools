# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import time

o_server = 'http://'
o_username = ''
o_password = ''
o_site_content_url = ''

logger = Logger('move.log')

d_server = 'http://'
d_username = ''
d_password = ''
d_site_content_url = ''

t = TableauRestApiConnection26(o_server, o_username, o_password, o_site_content_url)
t.signin()
t.enable_logging(logger)
downloaded_filename = 'File Name'
wb_name_on_server = 'WB Name on Server'
proj_name = 'Default'
t.download_workbook('WB Name on Server', downloaded_filename, proj_name_or_luid=proj_name)

d = TableauRestApiConnection25(d_server, d_username, d_password, d_site_content_url)
d.signin()
d.enable_logging(logger)
proj = d.query_project('Default')
d.publish_workbook('{}.twbx'.format(downloaded_filename), wb_name_on_server, proj, save_credentials=False, overwrite=True)