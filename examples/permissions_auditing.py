# -*- coding: utf-8 -*-
from tableau_tools.tableau_rest_api import *
from tableau_tools.tableau_rest_api.published_content import Project, Workbook, Datasource
from tableau_tools import *

username = ''
password = ''
server = 'http://localhost'

logger = Logger('permissions.log')
default = TableauRestApiConnection(server, username, password)
default.enable_logging(logger)
default.signin()

output_file = open('permissions_audit.txt', 'wb')
# Get all sites content urls for logging in
site_content_urls = default.query_all_site_content_urls()

# Headers
output_file.write(u'Site Content URL,Project Name,Project LUID,Principal Type,Principal Name,Principal LUID')
project_caps = default.available_capabilities[default.api_version]['project']
for cap in project_caps:
    output_file.write(u',{}'.format(cap))
workbook_caps = default.available_capabilities[default.api_version]['workbook']
for cap in workbook_caps:
    output_file.write(u',{}'.format(cap))
datasource_caps = default.available_capabilities[default.api_version]['datasource']
for cap in datasource_caps:
    output_file.write(u',{}'.format(cap))
output_file.write("\n")

for site_content_url in site_content_urls:
    t = TableauRestApiConnection(server, username, password, site_content_url)
    t.enable_logging(logger)
    t.signin()
    projects = t.query_projects()
    projects_dict = t.convert_xml_list_to_name_id_dict(projects)
    for project in projects_dict:
        # combined_permissions = luid : {type, name, proj, def_wb, def_ds}
        gcap_combined_permissions = {}
        proj_obj = t.get_project_object_by_luid(projects_dict[project])
        # is_locked = proj_obj.
        all_perms = proj_obj.query_all_permissions()

        for luid in all_perms:
            if site_content_url is None:
                site_content_url = ''
            output_file.write(u'{}'.format(site_content_url).encode('utf-8'))
            output_file.write(u",{},{}".format(project, projects_dict[project]).encode('utf-8'))
            output_file.write(u",{},{},{}".format(all_perms[luid]["type"], all_perms[luid]["name"], luid).encode('utf-8'))
            all_perms_list = proj_obj.convert_all_permissions_to_list(all_perms[luid])
            for perm in all_perms_list:
                output_file.write(u",{}".format(unicode(perm)))
            output_file.write('\n')






