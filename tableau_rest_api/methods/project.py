from .rest_api_base import *
from ..published_content import *

class ProjectMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest = rest_api_base

    #def __getattr__(self, attr):
    #    return getattr(self.rest_api_base, attr)

    def query_projects(self, name_filter: Optional[UrlFilter] = None, owner_name_filter: Optional[UrlFilter] = None,
                       updated_at_filter: Optional[UrlFilter] = None, created_at_filter: Optional[UrlFilter] = None,
                       owner_domain_filter: Optional[UrlFilter] = None, owner_email_filter: Optional[UrlFilter] = None,
                       sorts: Optional[List[Sort]] = None) -> ET.Element:
        filter_checks = {'name': name_filter, 'ownerName': owner_name_filter,
                         'updatedAt': updated_at_filter, 'createdAt': created_at_filter,
                         'ownerDomain': owner_domain_filter, 'ownerEmail': owner_email_filter}

        filters = self.rest._check_filter_objects(filter_checks)

        self.rest.start_log_block()
        projects = self.rest.query_resource("projects", filters=filters, sorts=sorts)
        self.rest.end_log_block()
        return projects

    def query_projects_json(self, name_filter: Optional[UrlFilter] = None,
                            owner_name_filter: Optional[UrlFilter] = None,
                            updated_at_filter: Optional[UrlFilter] = None,
                            created_at_filter: Optional[UrlFilter] = None,
                            owner_domain_filter: Optional[UrlFilter] = None,
                            owner_email_filter: Optional[UrlFilter] = None, sorts: Optional[List[Sort]] = None,
                            page_number: Optional[int] = None) -> Dict:
        filter_checks = {'name': name_filter, 'ownerName': owner_name_filter,
                         'updatedAt': updated_at_filter, 'createdAt': created_at_filter,
                         'ownerDomain': owner_domain_filter, 'ownerEmail': owner_email_filter}

        filters = self.rest._check_filter_objects(filter_checks)

        self.rest.start_log_block()
        projects = self.rest.query_resource_json("projects", filters=filters, sorts=sorts, page_number=None)
        self.rest.end_log_block()
        return projects

    def query_project(self, project_name_or_luid: str) -> Project:

        self.rest.start_log_block()
        luid = self.rest.query_project_luid(project_name_or_luid)
        # Project endpoint can be filtered on Project Name, but not project LUID in direct API request
        proj = self.get_published_project_object(luid, self.rest.query_single_element_from_endpoint('project',
                                                                                               luid))
        self.rest.end_log_block()
        return proj

    def get_published_project_object(self, project_name_or_luid: str,
                                     project_xml_obj: Optional[ET.Element] = None) -> Project:

        luid = self.rest.query_project_luid(project_name_or_luid)

        parent_project_luid = None
        if project_xml_obj.get('parentProjectId'):
            parent_project_luid = project_xml_obj.get('parentProjectId')

        proj_obj = Project(luid=luid, tableau_rest_api_obj=self.rest,
                             logger_obj=self.rest.logger, content_xml_obj=project_xml_obj,
                             parent_project_luid=parent_project_luid)
        return proj_obj

    def create_project(self, project_name: Optional[str] = None, parent_project_name_or_luid: Optional[str] = None,
                       project_desc: Optional[str] = None, locked_permissions: bool = True,
                       publish_samples: bool = False, no_return: bool = False,
                       direct_xml_request: Optional[ET.Element] = None) -> Project:

        self.rest.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = ET.Element("tsRequest")
            p = ET.Element("project")
            p.set("name", project_name)

            if project_desc is not None:
                p.set('description', project_desc)
            if locked_permissions is not False:
                p.set('contentPermissions', "LockedToProject")

            if parent_project_name_or_luid is not None:
                parent_project_luid = self.rest.query_project_luid(parent_project_name_or_luid)
                p.set('parentProjectId', parent_project_luid)
            tsr.append(p)

        url = self.rest.build_api_url("projects")
        if publish_samples is True:
            url += '?publishSamples=true'
        try:
            new_project = self.rest.send_add_request(url, tsr)
            self.rest.end_log_block()
            project_luid = new_project.findall('.//t:project', self.rest.ns_map)[0].get("id")
            if no_return is False:
                proj_obj = self.get_published_project_object(project_luid, new_project)

                return proj_obj
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.rest.log('Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.rest.end_log_block()
                if no_return is False:
                    return self.rest.query_project(project_name)



    def query_project_xml_object(self, project_name_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        luid = self.rest.query_project_luid(project_name_or_luid)
        proj_xml = self.rest.query_single_element_from_endpoint_with_filter('project', luid)
        self.rest.end_log_block()
        return proj_xml

    def update_project(self, name_or_luid: str, parent_project_name_or_luid: Optional[str] = None,
                       new_project_name: Optional[str] = None, new_project_description: Optional[str] = None,
                       locked_permissions: Optional[bool] = None, publish_samples: bool = False) -> Project:

        self.rest.start_log_block()
        project_luid = self.rest.query_project_luid(name_or_luid)

        tsr = ET.Element("tsRequest")
        p = ET.Element("project")
        if new_project_name is not None:
            p.set('name', new_project_name)
        if new_project_description is not None:
            p.set('description', new_project_description)
        if parent_project_name_or_luid is not None:
            parent_project_luid = self.rest.query_project_luid(parent_project_name_or_luid)
            p.set('parentProjectId', parent_project_luid)
        if locked_permissions is True:
            p.set('contentPermissions', "LockedToProject")
        elif locked_permissions is False:
            p.set('contentPermissions', "ManagedByOwner")

        tsr.append(p)

        url = self.rest.build_api_url("projects/{}".format(project_luid))
        if publish_samples is True:
            url += '?publishSamples=true'

        response = self.rest.send_update_request(url, tsr)
        proj_xml_obj = response.findall(".//t:project", TableauRestXml.ns_map)[0]
        self.rest.end_log_block()
        return self.get_published_project_object(project_name_or_luid=project_luid, project_xml_obj=proj_xml_obj)

    def delete_projects(self, project_name_or_luid_s: Union[List[str], str]):
        self.rest.start_log_block()
        projects = self.rest.to_list(project_name_or_luid_s)
        for project_name_or_luid in projects:
            project_luid = self.rest.query_project_luid(project_name_or_luid)
            url = self.rest.build_api_url("projects/{}".format(project_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()


class ProjectMethods33(ProjectMethods):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest = rest_api_base


    def get_published_project_object(self, project_name_or_luid: str,
                                     project_xml_obj: Optional[ET.Element] = None) -> Project33:

        luid = self.rest.query_project_luid(project_name_or_luid)

        parent_project_luid = None
        if project_xml_obj.get('parentProjectId'):
            parent_project_luid = project_xml_obj.get('parentProjectId')

        proj_obj = Project33(luid=luid, tableau_rest_api_obj=self.rest,
                             logger_obj=self.rest.logger, content_xml_obj=project_xml_obj,
                             parent_project_luid=parent_project_luid)
        return proj_obj

    # These are only reimplemented in the sense that they return Project33 vs. Project objects
    def query_project(self, project_name_or_luid: str) -> Project33:

        self.rest.start_log_block()
        luid = self.rest.query_project_luid(project_name_or_luid)
        # Project endpoint can be filtered on Project Name, but not project LUID in direct API request
        proj = self.get_published_project_object(luid, self.rest.query_single_element_from_endpoint('project',
                                                                                               luid))
        self.rest.end_log_block()
        return proj

    def create_project(self, project_name: Optional[str] = None, parent_project_name_or_luid: Optional[str] = None,
                       project_desc: Optional[str] = None, locked_permissions: bool = True,
                       publish_samples: bool = False, no_return: bool = False,
                       direct_xml_request: Optional[ET.Element] = None) -> Project33:

        self.rest.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = ET.Element("tsRequest")
            p = ET.Element("project")
            p.set("name", project_name)

            if project_desc is not None:
                p.set('description', project_desc)
            if locked_permissions is not False:
                p.set('contentPermissions', "LockedToProject")

            if parent_project_name_or_luid is not None:
                parent_project_luid = self.rest.query_project_luid(parent_project_name_or_luid)
                p.set('parentProjectId', parent_project_luid)
            tsr.append(p)

        url = self.rest.build_api_url("projects")
        if publish_samples is True:
            url += '?publishSamples=true'
        try:
            new_project = self.rest.send_add_request(url, tsr)
            self.rest.end_log_block()
            project_luid = new_project.findall('.//t:project', self.rest.ns_map)[0].get("id")
            if no_return is False:
                proj_obj = self.get_published_project_object(project_luid, new_project)

                return proj_obj
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.rest.log('Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.rest.end_log_block()
                if no_return is False:
                    return self.rest.query_project(project_name)

    def update_project(self, name_or_luid: str, parent_project_name_or_luid: Optional[str] = None,
                       new_project_name: Optional[str] = None, new_project_description: Optional[str] = None,
                       locked_permissions: Optional[bool] = None, publish_samples: bool = False) -> Project33:

        self.rest.start_log_block()
        project_luid = self.rest.query_project_luid(name_or_luid)

        tsr = ET.Element("tsRequest")
        p = ET.Element("project")
        if new_project_name is not None:
            p.set('name', new_project_name)
        if new_project_description is not None:
            p.set('description', new_project_description)
        if parent_project_name_or_luid is not None:
            parent_project_luid = self.rest.query_project_luid(parent_project_name_or_luid)
            p.set('parentProjectId', parent_project_luid)
        if locked_permissions is True:
            p.set('contentPermissions', "LockedToProject")
        elif locked_permissions is False:
            p.set('contentPermissions', "ManagedByOwner")

        tsr.append(p)

        url = self.rest.build_api_url("projects/{}".format(project_luid))
        if publish_samples is True:
            url += '?publishSamples=true'

        response = self.rest.send_update_request(url, tsr)
        proj_xml_obj = response.findall(".//t:project", TableauRestXml.ns_map)[0]
        self.rest.end_log_block()
        return self.get_published_project_object(project_name_or_luid=project_luid, project_xml_obj=proj_xml_obj)

