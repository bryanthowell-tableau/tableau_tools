# -*- coding: utf-8 -*-
from tableau_tools import *
import csv

username = ''
password = ''
server = 'http://localhost'

logger = Logger('permissions.log')
default = TableauRestApiConnection28(server=server, username=username, password=password)
default.enable_logging(logger)
default.signin()

with open('permissions_audit.txt', 'wb') as output_file:

    # Get all sites content urls for logging in
    site_content_urls = default.query_all_site_content_urls()

    # Create CSV writer
    output_writer = csv.writer(output_file)

    # Headers
    headers = ['Site Content URL', 'Project Name', 'Project LUID', 'Are Permissions Locked?',
               'Principal Type', 'Principal Name', 'Principal LUID']

    project_caps = default.available_capabilities[default.api_version]['project']
    for cap in project_caps:
        headers.append(',{}'.format(cap))
    workbook_caps = default.available_capabilities[default.api_version]['workbook']
    for cap in workbook_caps:
        headers.append(',{}'.format(cap))
    datasource_caps = default.available_capabilities[default.api_version]['datasource']
    for cap in datasource_caps:
        headers.append(',{}'.format(cap))
    output_writer.writerow(headers)

    for site_content_url in site_content_urls:
        t = TableauRestApiConnection28(server=server, username=username, password=password,
                                       site_content_url=site_content_url)
        t.enable_logging(logger)
        t.signin()
        projects = t.query_projects()
        projects_dict = t.xml_list_to_dict(projects)
        print(projects_dict)
        for project in projects_dict:
            # combined_permissions = luid : {type, name, proj, def_wb, def_ds}
            proj_obj = t.query_project(projects_dict[project])

            all_perms = proj_obj.query_all_permissions()

            for luid in all_perms:
                output_row = []
                all_perms_list = proj_obj.convert_all_permissions_to_list(all_perms[luid])
                if site_content_url is None:
                    site_content_url = ''
                output_row.append(site_content_url.encode('utf-8'))
                output_row.append(project.encode('utf-8'))
                output_row.append(projects_dict[project].encode('utf-8'))
                output_row.append(str(proj_obj.are_permissions_locked()))
                output_row.append(all_perms[luid]["type"].encode('utf-8'))
                output_row.append(all_perms[luid]["name"].encode('utf-8'))
                output_row.append(luid.encode('utf-8'))
                output_row.extend(all_perms_list)
                output_writer.writerow(output_row)






