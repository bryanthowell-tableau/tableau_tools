# -*- coding: utf-8 -*-
from tableau_tools import *
from tableau_tools.tabcmd import Tabcmd
from tableau_tools.tableau_rest_api import *

logger = Logger(u'extract_refresh.log')


tabcmd_dir = "C:\\tabcmd\\Command Line Utility\\"
tabcmd_config_location = 'C:\\Users\\{}\\AppData\\Local\\Tableau\\Tabcmd\\'

server = 'http://127.0.0.1'
site_content_url = 'default'
username = '{}'
password = '{}'

tabcmd = Tabcmd(tabcmd_dir, server, username, password, site=site_content_url, tabcmd_config_location=tabcmd_config_location)
tabcmd.enable_logging(logger)

# Trigger a schedule to run
tabcmd.trigger_schedule_run('Disabled Schedule')

t = TableauRestApiConnection(server, username, password, site_content_url=site_content_url)
t.enable_logging(logger)

t.signin()
# Triggering all data sources to refresh
dses = t.query_datasources()
for ds in dses:
    datasource_name = ds.get('name')
    for element in ds.iter():
        if element.tag.find('project') != -1:
            project_name = element.get('name')

    tabcmd.trigger_extract_refresh(project_name, 'datasource', datasource_name)

# Triggering all workbooks to refresh
wbs = t.query_workbooks()
for wb in wbs:
    workbook_name = wb.get('name')
    for element in wb.iter():
        if element.tag.find('project') != -1:
            project_name = element.get('name')

    tabcmd.trigger_extract_refresh(project_name, 'workbook', workbook_name)


