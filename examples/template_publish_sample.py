# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools.tableau_documents import *
from tableau_tools import *
from tableau_tools.tableau_repository import TableauRepository
import urllib2
import time
import datetime
import os

logger = Logger(u'template_publish.txt')

# In production, you would pull this from a config file or database
tableau_sites = [
    {u'server': u'http://tableauserver1', u'username': u'username', u'password': u'password',
     u'site_content_url': u'site1', u'db_server': u'dbserv1', u'db_name': u'db1',
     u'db_user': u'db1user', u'db_password': u'db1pass'},
    {u'server': u'http://tableauserver1', u'username': u'username', u'password': u'password',
     u'site_content_url': u'site2', u'db_server': u'dbserv1', u'db_name': u'db2',
     u'db_user': u'db2user', u'db_password': u'db2pass'},
    {u'server': u'http://tableauserver2', u'username': u'username', u'password': u'password',
     u'site_content_url': u'site3', u'db_server': u'dbserv2', u'db_name': u'db3',
     u'db_user': u'db3user', u'db_password': u'db3pass'}
]


def promote_from_dev_to_test(logger_obj=None):
    dev_server = u'http://'
    dev_username = u''
    dev_password = u'a'
    dev_site = u'dev'

    test_server = u'http://'
    test_username = u''
    test_password = u''
    test_site = u'test'

    dev = TableauRestApiConnection26(dev_server, dev_username, dev_password, dev_site)
    dev.signin()

    test = TableauRestApiConnection26(test_server, test_username, test_password, test_site)
    test.signin()

    dev.enable_logging(logger_obj)
    test.enable_logging(logger_obj)

    # Create a filter that was last updated by
    today = datetime.datetime.now()
    offset_time = datetime.timedelta(days=15)
    time_to_filter_by = today - offset_time
    # Tableau Time Filters require this format: YYYY-MM-DDTHH:MM:SSZ
    filter_time_string = time_to_filter_by.isoformat('T')[:19] + 'Z'

    last_update_filter = UrlFilter.create_updated_at_filter(u'gt', filter_time_string)

    # Download from Templates for publishing Project
    content_to_promote_project_name = u'Content to Promote'
    dses = dev.query_datasources(content_to_promote_project_name, updated_at_filter=last_update_filter)
    ds_dict = dev.convert_xml_list_to_name_id_dict(dses)
    for ds in ds_dict:
        print u"Downloading Datasource {}".format(ds)
        ds_file = dev.download_datasource(ds_dict[ds], ds, content_to_promote_project_name)
        t_file = TableauFile(ds_file, logger_obj)
        dses = t_file.tableau_document.datasources
        for d in dses:
            for conn in d.connections:
                print conn.connection_type
                print conn.dbname
                if conn.dbname == u'dev_db':
                    conn.dbname = u'test_db'

        t_file.save_new_file(u'Temp TDSX')
        new_project = test.query_project(u'Promoted Content')
        test.publish_datasource(u'Temp TDSX.tdsx', ds, new_project, overwrite=True, save_credentials=True)
        # If you have credentials to publish
        # test.publish_datasource(temp_filename, ds, new_project, connection_username=u'', connection_password=u'', overwrite=True, save_credentials=True)
        os.remove(u'Temp TDSX.tdsx')

# promote_from_dev_to_test(logger)

# Uses experimental Deployer object, so commented out for now. The logic is sound though
"""
def publish_from_project_on_dev_server_to_multiple_sites(logger_obj=None):

    dev_server = u'http://'
    dev_username = u''
    dev_password = u''
    dev_site = u'dev'
    dev = TableauRestApiConnection26(dev_server, dev_username, dev_password, dev_site)
    dev.signin()
    dev.enable_logging(logger_obj)

    # Log into each Tableau Site, put into the ContentDeployer
    deployer = ContentDeployer()
    for site in tableau_sites:
        t = TableauRestApiConnection26(site[u'server'], site[u'username'], site[u'password'],
                                       site[u'site_content_url'])
        t.signin()
        t.enable_logging(logger_obj)
        deployer.add_site(t)

    # Create a filter that was last updated by
    today = datetime.datetime.now()
    offset_time = datetime.timedelta(days=15)
    time_to_filter_by = today - offset_time
    # Tableau Time Filters require this format: YYYY-MM-DDTHH:MM:SSZ
    filter_time_string = time_to_filter_by.isoformat('T')[:19] + 'Z'

    last_update_filter = UrlFilter.create_updated_at_filter(u'gt', filter_time_string)

    content_to_promote_project_name = u'Content to Promote'
    dses = dev.query_datasources(content_to_promote_project_name, updated_at_filter=last_update_filter)
    ds_dict = dev.convert_xml_list_to_name_id_dict(dses)
    for ds in ds_dict:
        print u"Downloading Datasource {}".format(ds)
        ds_file = dev.download_datasource(ds_dict[ds], ds, content_to_promote_project_name)
        t_file = TableauFile(ds_file, logger_obj)
        dses = t_file.tableau_document.datasources
        # Loop through each Tableau Site to make the correct changes and deploy
        # Notice we keep the same TableauFile object open, but are saving new copies as Temp TDSX.tdsx to publish with
        # each change
        for site in tableau_sites:
            deployer.current_site = site[u'site_content_url']
            t = deployer.current_site
            for d in dses:
                for conn in d.connections:
                    conn.dbname = site[u'db_name']
                    conn.server = site[u'db_server']

            t_file.save_new_file(u'Temp TDSX')
            # Loop through each of the sites to deploy

            new_project = t.query_project(u'Content')
            print u"Publishing to site {}".format(t.site_content_url)
            t.publish_datasource(u'Temp TDSX.tdsx', ds, new_project, overwrite=True, save_credentials=True,
                                 connection_username=site[u'db_user'], connection_password=site[u'db_password'])
            os.remove(u'Temp TDSX.tdsx')
    # Sign out of all sites
    for site in deployer:
        site.current_site.signout()
"""


def publish_from_live_connections_to_extracts(logger_obj=None):
    # This one goes from a file on disk, as opposed to downloading from dev or test site. This simulates
    # a scenario where you are using source control rather than Tableau Server.

    # Assume you might do this for a whole directory, just showing a single file
    t_file = TableauFile(u'SS.tds', logger_obj)
    dses = t_file.tableau_document.datasources
    for ds in dses:
        cols = ds.xml.find(u'.//cols')
        for m in cols:
            print m.get(u'value')

        for conn in ds.connections:
            conn.dbname = u'Global SuperStore Star Schema - Staging'

        ds.add_extract(u'Extract File.tde')
        ds.add_dimension_extract_filter(u'Customer Segment', [u'Home Office'])
    new_filename = t_file.save_new_file(u'Saved Source')
    t = TableauRestApiConnection26(u'http://', u'', u'', site_content_url=u'test')
    t.signin()
    t.enable_logging(logger_obj)

    t2 = TableauRestApiConnection26(u'http://', u'', u'', site_content_url=u'tsite')
    t2.signin()
    t2.enable_logging(logger_obj)

    default_proj = t.query_project(u'Default')
    t.publish_datasource(new_filename, u'Auto TDSX', default_proj, overwrite=True, save_credentials=True)

    t2_default = t2.query_project(u'Default')
    t2.publish_datasource(new_filename, u'Auto TDSX', t2_default, overwrite=True, save_credentials=True)

    t_file = TableauFile(u'SS Example.twb')
    dses = t_file.tableau_document.datasources
    for ds in dses:
        #for conn in ds.connections:
        #    conn.dbname = u'Global SuperStore Star Schema - Staging'
        ds.add_extract(u'Extract File.tde')
        ds.add_dimension_extract_filter(u'Customer Segment', [u'Consumer'])
    new_filename = t_file.save_new_file(u'Saved Source')
    default_proj = t.query_project(u'Default')
    new_wb_luid = t.publish_workbook(new_filename, u'Auto TWBX', default_proj, overwrite=True, save_credentials=True)
    logger_obj.log(u'New LUID is {}'.format(new_wb_luid))

    # Here is where you would set this to be on an extract schedule via REST API, but there is no call for it

    # This is the super-sneaky way to do this (requires the super secret password)
    # Must be run from the Tableau Server itself, because superadmin connections are restrictd
    #tab_rep = TableauRepository(u'', repository_username=u'tblwgadmin', repository_password=u'')
    #tab_rep.set_workbook_on_schedule(new_wb_luid, u'Saturday night')

    # Trigger the refresh

    t.run_extract_refresh_for_workbook(new_wb_luid)

    # HEre's one with a published data source

    default_proj = t.query_project(u'Default')
    new_wb_luid = t.publish_workbook(u'Published DS Connect.twb', u'Published DS', default_proj, overwrite=True,
                                     save_credentials=True)
    logger_obj.log(u'New LUID is {}'.format(new_wb_luid))

    new_wb_luid = t2.publish_workbook(u'Published DS Connect.twb', u'Published DS', t2_default, overwrite=True,
                                      save_credentials=True)
    logger_obj.log(u'New LUID is {}'.format(new_wb_luid))

    # This TWBX has one extract and one live connection
    # This adds an extract to the live connection

    t_file = TableauFile(u'Complex.twbx', logger_obj)
    dses = t_file.tableau_document.datasources
    i = 1
    for ds in dses:
        #for conn in ds.connections:
        #    conn.dbname = u'Global SuperStore Star Schema - Staging'
        try:
            ds.add_extract(u'Extract {}.tde'.format(i))
            i += 1
        except AlreadyExistsException as e:
            continue
    new_filename = t_file.save_new_file(u'Complex Updated')
    #default_proj = t.query_project(u'Default')
    #new_wb_luid = t.publish_workbook(new_filename, u'Auto TWBX', default_proj, overwrite=True, save_credentials=True)
    #logger_obj.log(u'New LUID is {}'.format(new_wb_luid))

publish_from_live_connections_to_extracts(logger)

#def create_datasource_from_scratch(logger_obj=None):

