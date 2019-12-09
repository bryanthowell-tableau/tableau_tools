# -*- coding: utf-8 -*-

from tableau_tools import *
from tableau_tools.tableau_documents import *

import datetime
import os

logger = Logger('template_publish.txt')

# In production, you would pull this from a config file or database
tableau_sites = [
    {'server': 'http://tableauserver1', 'username': 'username', 'password': 'password',
     'site_content_url': 'site1', 'db_server': 'dbserv1', 'db_name': 'db1',
     'db_user': 'db1user', 'db_password': 'db1pass'},
    {'server': 'http://tableauserver1', 'username': 'username', 'password': 'password',
     'site_content_url': 'site2', 'db_server': 'dbserv1', 'db_name': 'db2',
     'db_user': 'db2user', 'db_password': 'db2pass'},
    {'server': 'http://tableauserver2', 'username': 'username', 'password': 'password',
     'site_content_url': 'site3', 'db_server': 'dbserv2', 'db_name': 'db3',
     'db_user': 'db3user', 'db_password': 'db3pass'}
]

# This example takes from a Dev Site and promotes to a test Site (could be on different Tableau Servers)
# It uses a UrlFilter to look for things that were last updated within the past 15 days and only pulls those

def promote_from_dev_to_test(logger_obj=None):
    dev_server = 'http://'
    dev_username = ''
    dev_password = 'a'
    dev_site = 'dev'

    test_server = 'http://'
    test_username = ''
    test_password = ''
    test_site = 'test'

    dev = TableauServerRest33(dev_server, dev_username, dev_password, dev_site)
    dev.signin()

    test = TableauServerRest33(test_server, test_username, test_password, test_site)
    test.signin()

    dev.enable_logging(logger_obj)
    test.enable_logging(logger_obj)

    # Create a filter that was last updated by
    today = datetime.datetime.now()
    offset_time = datetime.timedelta(days=15)
    time_to_filter_by = today - offset_time
    # Tableau Time Filters require this format: YYYY-MM-DDTHH:MM:SSZ
    filter_time_string = time_to_filter_by.isoformat('T')[:19] + 'Z'

    last_update_filter = dev.url_filters.get_updated_at_filter('gt', filter_time_string)

    # Download from Templates for publishing Project
    content_to_promote_project_name = 'Content to Promote'
    dses = dev.datasources.query_datasources(content_to_promote_project_name, updated_at_filter=last_update_filter)
    ds_dict = dev.xml_list_to_dict(dses)
    for ds in ds_dict:
        print("Downloading Datasource {}".format(ds))
        ds_file = dev.datasources.download_datasource(ds_dict[ds], ds, content_to_promote_project_name)
        t_file = TableauFileManager.open(filename=ds_file, logger_obj=logger_obj)
        # Iterate through any data sources that are found
        dses = t_file.datasources
        for d in dses:
            # You can do things here on the data source itself (change the main table name or custom SQL)
            # See test_suite_tableau_documents for examples of these changes
            # Connection properties live within the connections collection, and can be read or modified
            for conn in d.connections:
                print(conn.connection_type)
                print(conn.dbname)
                # This is updating the name/schema of the database. Changes to tables / custom SQL / stored proc happens
                # up a level in the datasource portion
                if conn.dbname == 'dev_db':
                    conn.dbname = 'test_db'

        t_file.save_new_file('Temp TDSX')
        new_project = test.projects.query_project('Promoted Content')
        new_ds_luid = test.datasources.publish_datasource(ds_filename='Temp TDSX.tdsx', ds_name=ds,
                                                          project_obj=new_project, overwrite=True, save_credentials=True)
        # If you have credentials to publish
        #test.publish_datasource(temp_filename, ds, new_project, connection_username=u'', connection_password=u'', overwrite=True, save_credentials=True)
        os.remove('Temp TDSX.tdsx')

        # If there is an Extract that needs to be refreshed immediately with the changes in place
        test.extracts.run_extract_refresh_for_datasource(ds_name_or_luid=new_ds_luid)
        # Put the Extract on a Schedule if it was not previously
        test.schedules.add_datasource_to_schedule(ds_name_or_luid=new_ds_luid, schedule_name_or_luid='Some Schedule')

# promote_from_dev_to_test(logger)

# This function shows changing out a Hyper file in an existing packaged workbook
# It assumes you have used the Hyper API at least once to generate a Hyper file, then used Tableau Desktop to connect
# to that Hyper file, and saved a packaged file (TWBX or TDSX) with that file
def hyper_api_swap_example(logger_obj = None):
    newly_built_hyper_filename = 'Replacement Hyper File.hyper'
    t_file = TableauFileManager.open(filename='Packaged File.tdsx', logger_obj=logger_obj)
    filenames = t_file.get_filenames_in_package()
    for filename in filenames:
        # Find my Hyper file
        if filename.lower.find('.hyper') != -1:
            t_file.set_file_for_replacement(filename_in_package=filename,
                                            replacement_filname_on_disk=newly_built_hyper_filename)
            break   # Breaking here on a TDSX, but you could do a whole mapping I suppose to replace multiples in a TDSX

    t_file.save_new_file(new_filename_no_extension='Updated Packaged File')

# This function shows publishing multiple workbooks from a single project
# which will also publish across any published data sources that are linked
# This is a complex algorithm but is necessary for working with published data sources
def replicate_workbooks_with_published_dses(o_site_connection: TableauServerRest, d_site_connection: TableauServerRest,
                                            workbook_name_or_luids: List[str],
                                            orig_proj_name_or_luid: str, dest_proj_name_or_luid: str):
    o = o_site_connection
    d = d_site_connection

    # Simple object to keep all the details this requires straight
    class PublishedDSInfo:
        def __init__(self, orig_content_url):
            self.orig_content_url = orig_content_url
            self.orig_luid = None
            self.orig_name = None
            self.orig_proj_name = None
            self.new_luid = None
            self.new_content_url = None

    # Determine which published data sources to copy across
    # Do in a first step in case multiple workbooks are connected to the same
    # set of data sources
    orig_ds_content_url = {}
    error_wbs = []

    # This assumes that all of the workbooks are in a single project
    # which is necessary to make sure there is no name duplication
    wb_files = {}

    # Go through all the workbooks you'd identified to publish over
    for wb in workbook_name_or_luids:
        try:

            # We need the workbook downloaded even if there are no published data sources
            # But we have to open it up to see if there are any
            wb_filename = o.workbooks.download_workbook(wb_name_or_luid=wb, filename_no_extension=wb,
                                              proj_name_or_luid=orig_proj_name_or_luid)
            wb_files[wb] = wb_filename
            # Open up the file using the tableau_documents sub-module to find out if the
            # data sources are published. This is easier and more exact than using the REST API
            wb_obj = TableauFileManager.open(filename=wb_filename, logger_obj=logger)
            dses = wb_obj.datasources
            for ds in dses:
                # Add any published datasources to the dict with the object to hold the details
                if ds.published is True:
                    orig_ds_content_url[ds.published_ds_content_url] = PublishedDSInfo(ds.published_ds_content_url)
        except NoMatchFoundException as e:
            logger.log('Could not find a workbook with name or luid {}, skipping'.format(wb))
            error_wbs.append(wb)
        except MultipleMatchesFoundException as e:
            logger.log('wb {} had multiple matches found, skipping'.format(wb))
            error_wbs.append(wb)

    print("Found {} published data sources to move over".format(len(orig_ds_content_url)))

    # Look up all these data sources and find their LUIDs so they can be downloaded
    all_dses = o.datasources.query_datasources()

    for ds_content_url in orig_ds_content_url:
        print(ds_content_url)

        ds_xml = all_dses.findall('.//t:datasource[@contentUrl="{}"]'.format(ds_content_url), o.ns_map)
        if len(ds_xml) == 1:
            orig_ds_content_url[ds_content_url].orig_luid = ds_xml[0].get('id')
            orig_ds_content_url[ds_content_url].orig_name = ds_xml[0].get('name')

            for element in ds_xml[0]:
                if element.tag.find('project') != -1:
                    orig_ds_content_url[ds_content_url].orig_proj_name = element.get('name')
                    break
        else:
            # This really shouldn't be possible, so you might want to add a break point here
            print('Could not find matching datasource for contentUrl {}'.format(ds_content_url))

    print('Finished finding all of the info from the data sources')

    # Download those data sources and republish them
    # You need the credentials to republish, as always

    dest_project = d.projects.query_project(dest_proj_name_or_luid)

    for ds in orig_ds_content_url:
        ds_filename = o.datasources.download_datasource(orig_ds_content_url[ds].orig_luid, 'downloaded ds')
        proj_obj = d.projects.query_project(orig_ds_content_url[ds].orig_proj_name)

        ds_obj = TableauFileManager.open(filename=ds_filename)
        ds_dses = ds_obj.tableau_document.datasources
        # You may need to change details of the data source connections here
        # Uncomment below if you have things to change
        # for ds_ds in ds_dses:
        #    for conn in ds_ds.connections:
                # Change the dbname is most common
                # conn.dbname = u'prod'
                # conn.port = u'10000'

        new_ds_filename = ds_obj.save_new_file('Updated Datasource')

        orig_ds_content_url[ds].new_luid = d.datasources.publish_datasource(new_ds_filename,
                                                                            orig_ds_content_url[ds].orig_name,
                                                                            proj_obj, overwrite=True)
        print('Published data source, resulting in new luid {}'.format(orig_ds_content_url[ds].new_luid))
        os.remove(new_ds_filename)

        try:
            new_ds = d.datasources.query_datasource(orig_ds_content_url[ds].new_luid)
            orig_ds_content_url[ds].new_content_url = new_ds[0].get('contentUrl')
            print('New Content URL is {}'.format(orig_ds_content_url[ds].new_content_url))
        except RecoverableHTTPException as e:
            print(e.tableau_error_code)
            print(e.http_code)
            print(e.luid)

    print('Finished republishing all data sources to the new site')

    # Now that you have the new contentUrls that map to the original ones,
    # and you know the DSes have been pushed across, you can open up the workbook and
    # make sure that all of the contentUrls are correct
    for wb in wb_files:
        t_file = TableauFileManager.open(filename=wb_files[wb], logger_obj=logger)
        dses = t_file.datasources
        for ds in dses:
            if ds.is_published is True:
                # Set the Site of the published data source
                ds.published_ds_site = d_site_connection.site_content_url

                o_ds_content_url = ds.published_ds_content_url
                if o_ds_content_url in orig_ds_content_url:
                    ds.published_ds_content_url = orig_ds_content_url[o_ds_content_url].new_content_url
            # If the datasources AREN'T published, then you may need to change details directly here
            else:
                print('Not a published data source')
            #    for conn in ds.connections:
                    # Change the dbname is most common
                    # conn.dbname = u'prod'
                    # conn.port = u'10000'

        temp_wb_file = t_file.save_new_file('Modified Workbook'.format(wb))
        new_workbook_luid = d.workbooks.publish_workbook(workbook_filename=temp_wb_file, workbook_name=wb,
                                               project_obj=dest_project,
                                               overwrite=True, check_published_ds=False)
        print('Published new workbook {}'.format(new_workbook_luid))
        os.remove(temp_wb_file)

    print('Finished publishing all workbooks')
