# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import os


def archive_tableau_site(save_to_directory, server, username, password, site_content_url):
    # The last two digits of this constructor match to the version of API available on the Tableau Server
    t = TableauRestApiConnection30(server=server, username=username,
                                   password=password, site_content_url=site_content_url)
    t.signin()
    all_projects = t.query_projects()
    all_projects_dict = t.convert_xml_list_to_name_id_dict(all_projects)

    # This gives you the Project name; the values of the dict are the LUIDs
    for project in all_projects_dict:
        # Create directory for projects
        try:
            print(u'Making directory {}'.format(project))
            os.mkdir(u'{}/{}'.format(save_to_directory, project))
        except OSError as e:
            print(u'Directory already exists')

        print(u'Downloading datasources for project {}'.format(project))
        # Get All Data sources
        dses_in_project = t.query_datasources(project_name_or_luid=all_projects_dict[project])
        for ds in dses_in_project:
            ds_luid = ds.get(u'id')
            ds_content_url = ds.get(u'contentUrl')
            print(u'Downloading datasource {}'.format(ds_content_url))
            t.download_datasource(ds_name_or_luid=ds_luid,
                                  filename_no_extension=u"{}/{}/{}".format(save_to_directory, project, ds_content_url),
                                  include_extract=False)

        print(u'Downloading workbooks for project {}'.format(project))
        wbs_in_project = t.query_workbooks_in_project(project_name_or_luid=all_projects_dict[project])
        for wb in wbs_in_project:
            wb_luid = wb.get(u'id')
            wb_content_url = wb.get(u'contentUrl')
            print(u'Downloading workbook {}'.format(wb_content_url))
            t.download_workbook(wb_name_or_luid=wb_luid,
                                filename_no_extension=u"{}/{}/{}".format(save_to_directory, project, wb_content_url),
                                include_extract=False)