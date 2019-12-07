# -*- coding: utf-8 -*-

# from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import os


def archive_tableau_site(save_to_directory, server, username, password, site_content_url):
    # The last two digits of this constructor match to the version of API available on the Tableau Server
    t = TableauServerRest33(server=server, username=username,
                                   password=password, site_content_url=site_content_url)
    t.signin()
    all_projects = t.projects.query_projects()
    all_projects_dict = t.xml_list_to_dict(all_projects)

    # This gives you the Project name; the values of the dict are the LUIDs
    for project in all_projects_dict:
        # Create directory for projects
        try:
            print('Making directory {}'.format(project))
            os.mkdir('{}/{}'.format(save_to_directory, project))
        except OSError as e:
            print('Directory already exists')

        print('Downloading datasources for project {}'.format(project))
        # Get All Data sources
        dses_in_project = t.datasources.query_datasources(project_name_or_luid=all_projects_dict[project])
        for ds in dses_in_project:
            ds_luid = ds.get('id')
            ds_content_url = ds.get('contentUrl')
            print('Downloading datasource {}'.format(ds_content_url))
            t.datasources.download_datasource(ds_name_or_luid=ds_luid,
                                  filename_no_extension="{}/{}/{}".format(save_to_directory, project, ds_content_url),
                                  include_extract=False)

        print('Downloading workbooks for project {}'.format(project))
        wbs_in_project = t.workbooks.query_workbooks_in_project(project_name_or_luid=all_projects_dict[project])
        for wb in wbs_in_project:
            wb_luid = wb.get('id')
            wb_content_url = wb.get('contentUrl')
            print(('Downloading workbook {}'.format(wb_content_url)))
            t.workbooks.download_workbook(wb_name_or_luid=wb_luid,
                                filename_no_extension="{}/{}/{}".format(save_to_directory, project, wb_content_url),
                                include_extract=False)