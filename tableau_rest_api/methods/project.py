from .rest_api_base import *
from ..published_content import *

class ProjectMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def query_projects(self) -> ET.Element:
        self.start_log_block()
        projects = self.query_resource("projects")
        self.end_log_block()
        return projects

    def query_projects_json(self, page_number: Optional[int] = None) -> Dict:
        self.start_log_block()
        projects = self.query_resource_json("projects", page_number=page_number)
        self.end_log_block()
        return projects

    def query_project(self, project_name_or_luid: str) -> Project:
        self.start_log_block()
        luid = self.query_project_luid(project_name_or_luid)
        # Project endpoint can be filtered on Project Name, but not project LUID in direct API request
        proj = self.get_published_project_object(luid, self.rest_api_base.query_single_element_from_endpoint('project',
                                                                                                             luid))
        self.end_log_block()
        return proj

    def get_published_project_object(self, project_name_or_luid: str,
                                     project_xml_obj: Optional[ET.Element] = None) -> Project:

        luid = self.query_project_luid(project_name_or_luid)

        proj_obj = Project(luid=luid, tableau_rest_api_obj=self,
                             logger_obj=self.logger, content_xml_obj=project_xml_obj
                           )
        return proj_obj

    def create_project(self, project_name: Optional[str] = None, project_desc: Optional[str] = None,
                       locked_permissions: bool = True, publish_samples: bool = False,
                       no_return: bool = False,
                       direct_xml_request: Optional[ET.Element] = None) -> Project:
        self.start_log_block()
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
            tsr.append(p)

        url = self.build_api_url("projects")
        if publish_samples is True:
            url += '?publishSamples=true'
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall('.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                return self.get_published_project_object(project_luid, new_project)
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log('Project named {} already exists, finding and returning the Published Project Object'.format(
                    project_name))
                self.end_log_block()
                if no_return is False:
                    return self.get_published_project_object(project_name_or_luid=project_name)



    def query_project_xml_object(self, project_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        luid = self.query_project_luid(project_name_or_luid)
        proj_xml = self.query_single_element_from_endpoint('project', luid)
        self.end_log_block()
        return proj_xml

    def update_project(self, name_or_luid: str, new_project_name: Optional[str] = None,
                       new_project_description: Optional[str] = None,
                       locked_permissions: Optional[bool] = None, publish_samples: bool = False) -> Project:
        self.start_log_block()
        project_luid = self.query_project_luid(name_or_luid)

        tsr = ET.Element("tsRequest")
        p = ET.Element("project")
        if new_project_name is not None:
            p.set('name', new_project_name)
        if new_project_description is not None:
            p.set('description', new_project_description)
        if locked_permissions is True:
            p.set('contentPermissions', "LockedToProject")
        elif locked_permissions is False:
            p.set('contentPermissions', "ManagedByOwner")

        tsr.append(p)

        url = self.build_api_url("projects/{}".format(project_luid))
        if publish_samples is True:
            url += '?publishSamples=true'

        response = self.send_update_request(url, tsr)
        proj_xml_obj = response.findall(".//t:project", TableauRestXml.ns_map)[0]
        self.end_log_block()
        return self.get_published_project_object(project_name_or_luid=project_luid, project_xml_obj=proj_xml_obj)

    def delete_projects(self, project_name_or_luid_s: Union[List[str], str]):
        self.start_log_block()
        projects = self.to_list(project_name_or_luid_s)
        for project_name_or_luid in projects:
            project_luid = self.query_project_luid(project_name_or_luid)
            url = self.build_api_url("projects/{}".format(project_luid))
            self.send_delete_request(url)
        self.end_log_block()


class ProjectMethods27(ProjectMethods):
    def __init__(self, rest_api_base: TableauRestApiBase27):
        self.rest_api_base = rest_api_base

    def query_projects(self, name_filter: Optional[UrlFilter] = None, owner_name_filter: Optional[UrlFilter] = None,
                       updated_at_filter: Optional[UrlFilter] = None, created_at_filter: Optional[UrlFilter] = None,
                       owner_domain_filter: Optional[UrlFilter] = None, owner_email_filter: Optional[UrlFilter] = None,
                       sorts: Optional[List[Sort]] = None) -> ET.Element:
        filter_checks = {'name': name_filter, 'ownerName': owner_name_filter,
                         'updatedAt': updated_at_filter, 'createdAt': created_at_filter,
                         'ownerDomain': owner_domain_filter, 'ownerEmail': owner_email_filter}

        filters = self._check_filter_objects(filter_checks)

        self.start_log_block()
        projects = self.query_resource("projects", filters=filters, sorts=sorts)
        self.end_log_block()
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

        filters = self._check_filter_objects(filter_checks)

        self.start_log_block()
        projects = self.query_resource_json("projects", filters=filters, sorts=sorts, page_number=None)
        self.end_log_block()
        return projects

    def query_project_xml_object(self, project_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        luid = self.query_project_luid(project_name_or_luid)
        proj_xml = self.query_single_element_from_endpoint_with_filter('project', luid)
        self.end_log_block()
        return proj_xml

class ProjectMethods28(ProjectMethods27):
    def __init__(self, rest_api_base: TableauRestApiBase28):
        self.rest_api_base = rest_api_base

    def get_published_project_object(self, project_name_or_luid: str,
                                     project_xml_obj: Optional[ET.Element] = None) -> Project28:

        luid = self.query_project_luid(project_name_or_luid)

        parent_project_luid = None
        if project_xml_obj.get('parentProjectId'):
            parent_project_luid = project_xml_obj.get('parentProjectId')

        proj_obj = Project28(luid=luid, tableau_rest_api_obj=self,
                             logger_obj=self.logger, content_xml_obj=project_xml_obj,
                             parent_project_luid=parent_project_luid)
        return proj_obj

    def create_project(self, project_name: Optional[str] = None, parent_project_name_or_luid: Optional[str] = None,
                       project_desc: Optional[str] = None, locked_permissions: bool = True,
                       publish_samples: bool = False, no_return: bool = False,
                       direct_xml_request: Optional[ET.Element] = None) -> Project28:

        self.start_log_block()
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
                parent_project_luid = self.query_project_luid(parent_project_name_or_luid)
                p.set('parentProjectId', parent_project_luid)
            tsr.append(p)

        url = self.build_api_url("projects")
        if publish_samples is True:
            url += '?publishSamples=true'
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall('.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                proj_obj = self.get_published_project_object(project_luid, new_project)

                return proj_obj
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log('Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.end_log_block()
                if no_return is False:
                    return self.query_project(project_name)

    def update_project(self, name_or_luid: str, parent_project_name_or_luid: Optional[str] = None,
                       new_project_name: Optional[str] = None, new_project_description: Optional[str] = None,
                       locked_permissions: Optional[bool] = None, publish_samples: bool = False) -> Project28:

        self.start_log_block()
        project_luid = self.query_project_luid(name_or_luid)

        tsr = ET.Element("tsRequest")
        p = ET.Element("project")
        if new_project_name is not None:
            p.set('name', new_project_name)
        if new_project_description is not None:
            p.set('description', new_project_description)
        if parent_project_name_or_luid is not None:
            parent_project_luid = self.query_project_luid(parent_project_name_or_luid)
            p.set('parentProjectId', parent_project_luid)
        if locked_permissions is True:
            p.set('contentPermissions', "LockedToProject")
        elif locked_permissions is False:
            p.set('contentPermissions', "ManagedByOwner")

        tsr.append(p)

        url = self.build_api_url("projects/{}".format(project_luid))
        if publish_samples is True:
            url += '?publishSamples=true'

        response = self.send_update_request(url, tsr)
        proj_xml_obj = response.findall(".//t:project", TableauRestXml.ns_map)[0]
        self.end_log_block()
        return self.get_published_project_object(project_name_or_luid=project_luid, project_xml_obj=proj_xml_obj)

    def query_project(self, project_name_or_luid: str) -> Project28:

        self.start_log_block()
        luid = self.query_project_luid(project_name_or_luid)
        # Project endpoint can be filtered on Project Name, but not project LUID in direct API request
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint('project',
                                                                                               luid))
        self.end_log_block()
        return proj

class ProjectMethods30(ProjectMethods28):
    def __init__(self, rest_api_base: TableauRestApiBase30):
        self.rest_api_base = rest_api_base


class ProjectMethods31(ProjectMethods30):
    def __init__(self, rest_api_base: TableauRestApiBase31):
        self.rest_api_base = rest_api_base


class ProjectMethods32(ProjectMethods31):
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base


class ProjectMethods33(ProjectMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base


    def get_published_project_object(self, project_name_or_luid: str,
                                     project_xml_obj: Optional[ET.Element] = None) -> Project33:

        luid = self.query_project_luid(project_name_or_luid)

        parent_project_luid = None
        if project_xml_obj.get('parentProjectId'):
            parent_project_luid = project_xml_obj.get('parentProjectId')

        proj_obj = Project33(luid=luid, tableau_rest_api_obj=self,
                             logger_obj=self.logger, content_xml_obj=project_xml_obj,
                             parent_project_luid=parent_project_luid)
        return proj_obj

    # These are only reimplemented in the sense that they return Project33 vs. Project28 objects
    def query_project(self, project_name_or_luid: str) -> Project33:

        self.start_log_block()
        luid = self.query_project_luid(project_name_or_luid)
        # Project endpoint can be filtered on Project Name, but not project LUID in direct API request
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint('project',
                                                                                               luid))
        self.end_log_block()
        return proj

    def create_project(self, project_name: Optional[str] = None, parent_project_name_or_luid: Optional[str] = None,
                       project_desc: Optional[str] = None, locked_permissions: bool = True,
                       publish_samples: bool = False, no_return: bool = False,
                       direct_xml_request: Optional[ET.Element] = None) -> Project33:

        self.start_log_block()
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
                parent_project_luid = self.query_project_luid(parent_project_name_or_luid)
                p.set('parentProjectId', parent_project_luid)
            tsr.append(p)

        url = self.build_api_url("projects")
        if publish_samples is True:
            url += '?publishSamples=true'
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall('.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                proj_obj = self.get_published_project_object(project_luid, new_project)

                return proj_obj
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log('Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.end_log_block()
                if no_return is False:
                    return self.query_project(project_name)

    def update_project(self, name_or_luid: str, parent_project_name_or_luid: Optional[str] = None,
                       new_project_name: Optional[str] = None, new_project_description: Optional[str] = None,
                       locked_permissions: Optional[bool] = None, publish_samples: bool = False) -> Project33:

        self.start_log_block()
        project_luid = self.query_project_luid(name_or_luid)

        tsr = ET.Element("tsRequest")
        p = ET.Element("project")
        if new_project_name is not None:
            p.set('name', new_project_name)
        if new_project_description is not None:
            p.set('description', new_project_description)
        if parent_project_name_or_luid is not None:
            parent_project_luid = self.query_project_luid(parent_project_name_or_luid)
            p.set('parentProjectId', parent_project_luid)
        if locked_permissions is True:
            p.set('contentPermissions', "LockedToProject")
        elif locked_permissions is False:
            p.set('contentPermissions', "ManagedByOwner")

        tsr.append(p)

        url = self.build_api_url("projects/{}".format(project_luid))
        if publish_samples is True:
            url += '?publishSamples=true'

        response = self.send_update_request(url, tsr)
        proj_xml_obj = response.findall(".//t:project", TableauRestXml.ns_map)[0]
        self.end_log_block()
        return self.get_published_project_object(project_name_or_luid=project_luid, project_xml_obj=proj_xml_obj)

class ProjectMethods34(ProjectMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class ProjectMethods35(ProjectMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base


class ProjectMethods36(ProjectMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base
