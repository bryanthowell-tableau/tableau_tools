# -*- coding: utf-8 -*-
from tableau_tools import *
from tableau_tools.tabcmd import Tabcmd
from tableau_tools.tableau_rest_api import *

# This script uses the tabcmd object to trigger an extract refresh
# Please use the 10_3 script if on a 10.3+ version of Tableau Server, which uses the REST API commands
# Should be totally deprecated at this point

logger = Logger(u'extract_refresh.log')

tabcmd_dir = u"C:\\tabcmd\\Command Line Utility\\"
tabcmd_config_location = u'C:\\Users\\{}\\AppData\\Local\\Tableau\\Tabcmd\\'

server = u'http://127.0.0.1'
site_content_url = u'default'
username = u'{}'
password = u'{}'

tabcmd = Tabcmd(tabcmd_dir, server, username, password, site=site_content_url, tabcmd_config_location=tabcmd_config_location)
tabcmd.enable_logging(logger)

# Trigger a schedule to run
tabcmd.trigger_schedule_run(u'Disabled Schedule')

t = TableauRestApiConnection(server, username, password, site_content_url=site_content_url)
t.enable_logging(logger)

t.signin()
# Triggering all data sources to refresh
dses = t.query_datasources()
for ds in dses:
    datasource_name = ds.get(u'name')
    for element in ds.iter():
        if element.tag.find(u'project') != -1:
            project_name = element.get(u'name')

    tabcmd.trigger_extract_refresh(project_name, u'datasource', datasource_name)

# Triggering all workbooks to refresh
wbs = t.query_workbooks()
for wb in wbs:
    workbook_name = wb.get(u'name')
    for element in wb.iter():
        if element.tag.find(u'project') != -1:
            project_name = element.get(u'name')

    tabcmd.trigger_extract_refresh(project_name, u'workbook', workbook_name)


