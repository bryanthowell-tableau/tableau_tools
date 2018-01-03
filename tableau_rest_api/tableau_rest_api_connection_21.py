from tableau_rest_api_connection import *


class TableauRestApiConnection21(TableauRestApiConnection):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"9.2")

    def get_published_project_object(self, project_name_or_luid, project_xml_obj=None):
        """
        :type project_name_or_luid: unicode
        :type project_xml_obj: project_xml_obj
        :rtype: Project21
        """
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj_obj = Project21(luid, self, self.version, self.logger, content_xml_obj=project_xml_obj)
        return proj_obj

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
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint(u'project', project_name_or_luid))

        self.end_log_block()
        return proj

    def create_project(self, project_name, project_desc=None, locked_permissions=True, no_return=False):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type locked_permissions: bool
        :type no_return: bool
        :rtype: Project21
        """
        self.start_log_block()

        tsr = etree.Element(u"tsRequest")
        p = etree.Element(u"project")
        p.set(u"name", project_name)

        if project_desc is not None:
            p.set(u'description', project_desc)
        if locked_permissions is not False:
            p.set(u'contentPermissions', u"LockedToProject")
        tsr.append(p)

        url = self.build_api_url(u"projects")
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall(u'.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                return self.get_published_project_object(project_luid, new_project)
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u'Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.end_log_block()
                if no_return is False:
                    return self.query_project(project_name)

    def update_project(self, name_or_luid, new_project_name=None, new_project_description=None,
                       locked_permissions=True):
        """
        :type name_or_luid: unicode
        :type new_project_name: unicode
        :type new_project_description: unicode
        :type locked_permissions: bool
        :rtype: Project21
        """
        self.start_log_block()
        if self.is_luid(name_or_luid):
            project_luid = name_or_luid
        else:
            project_luid = self.query_project_luid(name_or_luid)

        tsr = etree.Element(u"tsRequest")
        p = etree.Element(u"project")
        if new_project_name is not None:
            p.set(u'name', new_project_name)
        if new_project_description is not None:
            p.set(u'description', new_project_description)
        if locked_permissions is True:
            p.set(u'contentPermissions', u"LockedToProject")
        elif locked_permissions is False:
            p.set(u'contentPermissions', u"ManagedByOwner")

        tsr.append(p)

        url = self.build_api_url(u"projects/{}".format(project_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return self.get_published_project_object(project_luid, response)

    def delete_groups(self, group_name_or_luid_s):
        """
        :type group_name_or_luid_s: list[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        groups = self.to_list(group_name_or_luid_s)
        for group_name_or_luid in groups:
            if group_name_or_luid == u'All Users':
                self.log(u'Cannot delete All Users group, skipping')
                continue
            if self.is_luid(group_name_or_luid):
                group_luid = group_name_or_luid
            else:
                group_luid = self.query_group_luid(group_name_or_luid)
            url = self.build_api_url(u"groups/{}".format(group_luid))
            self.send_delete_request(url)
        self.end_log_block()