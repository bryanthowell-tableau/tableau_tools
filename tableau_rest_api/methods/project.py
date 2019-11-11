from .rest_api_base import *
class ProjectMethods(TableauRestApiBase):
    #
    # Start Project Querying methods
    #

    def query_projects(self) -> etree.Element:
        self.start_log_block()
        projects = self.query_resource("projects")
        self.end_log_block()
        return projects

    def query_projects_json(self, page_number: Optional[int] = None) -> str:
        self.start_log_block()
        projects = self.query_resource_json("projects", page_number=page_number)
        self.end_log_block()
        return projects

    def create_project(self, project_name: Optional[str] = None, project_desc: Optional[str] = None,
                       locked_permissions: bool = True, publish_samples: bool = False,
                       no_return: Optional[bool] = False,
                       direct_xml_request: Optional[etree.Element] = None) -> Project21:
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            p = etree.Element("project")
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

    def query_project_luid(self, project_name: str) -> str:
        self.start_log_block()
        project_luid = self.query_luid_from_name(content_type='project', name=project_name)
        self.end_log_block()
        return project_luid

    def query_project_xml_object(self, project_name_or_luid: str) -> etree.Element:
        self.start_log_block()
        luid = self.query_project_luid(project_name_or_luid)
        proj_xml = self.query_single_element_from_endpoint('project', luid)
        self.end_log_block()
        return proj_xml

    def create_project(self, project_name=None, project_desc=None, locked_permissions=True, no_return=False,
                       direct_xml_request=None):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type locked_permissions: bool
        :type no_return: bool
        :type direct_xml_request: etree.Element
        :rtype: Project21
        """
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            p = etree.Element("project")
            p.set("name", project_name)

            if project_desc is not None:
                p.set('description', project_desc)
            if locked_permissions is not False:
                p.set('contentPermissions', "LockedToProject")
            tsr.append(p)

        url = self.build_api_url("projects")
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

    #
    # End Project Querying Methods
    #

    # Simplest method
    def update_project(self, name_or_luid, new_project_name=None, new_project_description=None,
                       locked_permissions=None, publish_samples=False):
        """
        :type name_or_luid: unicode
        :type new_project_name: unicode
        :type new_project_description: unicode
        :type locked_permissions: bool
        :type publish_samples: bool
        :rtype: Project21
        """
        self.start_log_block()
        project_luid = self.query_project_luid(name_or_luid)

        tsr = etree.Element("tsRequest")
        p = etree.Element("project")
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
        self.end_log_block()
        return self.get_published_project_object(project_luid, response)

    def delete_projects(self, project_name_or_luid_s):
        """
        :type project_name_or_luid_s: List[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        projects = self.to_list(project_name_or_luid_s)
        for project_name_or_luid in projects:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            url = self.build_api_url("projects/{}".format(project_luid))
            self.send_delete_request(url)
        self.end_log_block()

class ProjectMethods27(ProjectMethods):
    def query_projects(self, name_filter=None, owner_name_filter=None, updated_at_filter=None, created_at_filter=None,
                       owner_domain_filter=None, owner_email_filter=None, sorts=None):
        """
        :type name_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type owner_domain_filter: UrlFilter
        :type owner_email_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        filter_checks = {'name': name_filter, 'ownerName': owner_name_filter,
                         'updatedAt': updated_at_filter, 'createdAt': created_at_filter,
                         'ownerDomain': owner_domain_filter, 'ownerEmail': owner_email_filter}

        filters = self._check_filter_objects(filter_checks)

        self.start_log_block()
        projects = self.query_resource("projects", filters=filters, sorts=sorts)
        self.end_log_block()
        return projects

    def query_projects_json(self, name_filter=None, owner_name_filter=None, updated_at_filter=None,
                            created_at_filter=None, owner_domain_filter=None, owner_email_filter=None, sorts=None,
                            page_number=None):
        """
        :type name_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type owner_domain_filter: UrlFilter
        :type owner_email_filter: UrlFilter
        :type sorts: list[Sort]
        :type page_number: int
        :rtype: etree.Element
        """
        filter_checks = {'name': name_filter, 'ownerName': owner_name_filter,
                         'updatedAt': updated_at_filter, 'createdAt': created_at_filter,
                         'ownerDomain': owner_domain_filter, 'ownerEmail': owner_email_filter}

        filters = self._check_filter_objects(filter_checks)

        self.start_log_block()
        projects = self.query_resource_json("projects", filters=filters, sorts=sorts, page_number=None)
        self.end_log_block()
        return projects

    def query_project_luid(self, project_name):
        """
        :type project_name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        project_luid = self.query_single_element_luid_from_endpoint_with_filter('project', project_name)
        self.end_log_block()
        return project_luid

    def query_project(self, project_name_or_luid):
        """
        :type project_name_or_luid: unicode
        :rtype: Project21
        """
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint_with_filter('project',
                                                                                                           project_name_or_luid))

        self.end_log_block()
        return proj

    def query_project_xml_object(self, project_name_or_luid):
        """
        :param project_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj_xml = self.query_single_element_from_endpoint_with_filter('project', luid)
        self.end_log_block()
        return proj_xml

class ProjectMethods28(ProjectMethods27):

    def get_published_project_object(self, project_name_or_luid, project_xml_obj=None):
        """
        :type project_name_or_luid: unicode
        :type project_xml_obj: project_xml_obj
        :rtype: Project28
        """
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)

        parent_project_luid = None
        if project_xml_obj.get('parentProjectId'):
            parent_project_luid = project_xml_obj.get('parentProjectId')

        proj_obj = Project28(luid, self, self.version, self.logger, content_xml_obj=project_xml_obj,
                             parent_project_luid=parent_project_luid)
        return proj_obj

    def create_project(self, project_name=None, parent_project_name_or_luid=None, project_desc=None, locked_permissions=True,
                       publish_samples=False, no_return=False, direct_xml_request=None):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type locked_permissions: bool
        :type publish_samples: bool
        :type no_return: bool
        :type parent_project_name_or_luid: unicode
        :type direct_xml_request: etree.Element
        :rtype: Project21
        """
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            p = etree.Element("project")
            p.set("name", project_name)

            if project_desc is not None:
                p.set('description', project_desc)
            if locked_permissions is not False:
                p.set('contentPermissions', "LockedToProject")

            if parent_project_name_or_luid is not None:
                if self.is_luid(parent_project_name_or_luid):
                    parent_project_luid = parent_project_name_or_luid
                else:
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

    def update_project(self, name_or_luid, parent_project_name_or_luid=None, new_project_name=None,
                       new_project_description=None, locked_permissions=None, publish_samples=False):
        """
        :type name_or_luid: unicode
        :type parent_project_name_or_luid: unicode
        :type new_project_name: unicode
        :type new_project_description: unicode
        :type locked_permissions: bool
        :type publish_samples: bool
        :rtype: Project28
        """
        self.start_log_block()
        if self.is_luid(name_or_luid):
            project_luid = name_or_luid
        else:
            project_luid = self.query_project_luid(name_or_luid)

        tsr = etree.Element("tsRequest")
        p = etree.Element("project")
        if new_project_name is not None:
            p.set('name', new_project_name)
        if new_project_description is not None:
            p.set('description', new_project_description)
        if parent_project_name_or_luid is not None:
            if self.is_luid(parent_project_name_or_luid):
                parent_project_luid = parent_project_name_or_luid
            else:
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
        self.end_log_block()
        return self.get_published_project_object(project_luid, response)

    def query_project(self, project_name_or_luid):
        """
        :type project_name_or_luid: unicode
        :rtype: Project28
        """
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint_with_filter('project',
                                                                                               project_name_or_luid))

        self.end_log_block()
        return proj

class ProjectMethods30(ProjectMethods28):
    pass

class ProjectMethods31(ProjectMethods30):
    pass

class ProjectMethods32(ProjectMethods31):
    pass

class ProjectMethods33(ProjectMethods32):
    pass

class ProjectMethods34(ProjectMethods33):
    pass