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
                       locked_permissions=None):
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

    def add_user_by_username(self, username=None, site_role=u'Unlicensed', auth_setting=None, update_if_exists=False,
                             direct_xml_request=None):
        """
        :type username: unicode
        :type site_role: unicode
        :type update_if_exists: bool
        :type auth_setting: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()

        # Check to make sure role that is passed is a valid role in the API
        if site_role not in self.site_roles:
            raise InvalidOptionException(u"{} is not a valid site role in Tableau Server".format(site_role))

        if auth_setting is not None:
            if auth_setting not in [u'SAML', u'ServerDefault']:
                raise InvalidOptionException(u'auth_setting must be either "SAML" or "ServerDefault"')
        self.log(u"Adding {}".format(username))
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element(u"tsRequest")
            u = etree.Element(u"user")
            u.set(u"name", username)
            u.set(u"siteRole", site_role)
            if auth_setting is not None:
                u.set(u'authSetting', auth_setting)
            tsr.append(u)

        url = self.build_api_url(u'users')
        try:
            new_user = self.send_add_request(url, tsr)
            new_user_luid = new_user.findall(u'.//t:user', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_user_luid
        # If already exists, update site role unless overridden.
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u"Username '{}' already exists on the server".format(username))
                if update_if_exists is True:
                    self.log(u'Updating {} to site role {}'.format(username, site_role))
                    self.update_user(username, site_role=site_role)
                    self.end_log_block()
                    return self.query_user_luid(username)
                else:
                    self.end_log_block()
                    raise AlreadyExistsException(u'Username already exists ', self.query_user_luid(username))
        except:
            self.end_log_block()
            raise

    # This is "Add User to Site", since you must be logged into a site.
    # Set "update_if_exists" to True if you want the equivalent of an 'upsert', ignoring the exceptions
    def add_user(self, username=None, fullname=None, site_role=u'Unlicensed', password=None, email=None, auth_setting=None,
                 update_if_exists=False, direct_xml_request=None):
        """
        :type username: unicode
        :type fullname: unicode
        :type site_role: unicode
        :type password: unicode
        :type email: unicode
        :type update_if_exists: bool
        :type auth_setting: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()

        try:
            # Add username first, then update with full name
            if direct_xml_request is not None:
                # Parse to second level, should be
                new_user_tsr = etree.Element(u'tsRequest')
                new_user_u = etree.Element(u'user')
                for t in direct_xml_request:
                    if t.tag != u'user':
                        raise InvalidOptionException(u'Must submit a tsRequest with a user element')
                    for a in t.attrib:
                        if a in [u'name', u'siteRole', u'authSetting']:
                            new_user_u.set(a, t.attrib[a])
                new_user_tsr.append(new_user_u)
                new_user_luid = self.add_user_by_username(direct_xml_request=new_user_tsr)

                update_tsr = etree.Element(u'tsRequest')
                update_u = etree.Element(u'user')
                for t in direct_xml_request:
                    for a in t.attrib:
                        if a in [u'fullName', u'email', u'password', u'siteRole', u'authSetting']:
                            update_u.set(a, t.attrib[a])
                update_tsr.append(update_u)
                self.update_user(username_or_luid=new_user_luid, direct_xml_request=update_tsr)

            else:
                new_user_luid = self.add_user_by_username(username, site_role=site_role,
                                                          update_if_exists=update_if_exists, auth_setting=auth_setting)
                self.update_user(new_user_luid, fullname, site_role, password, email)
            self.end_log_block()
            return new_user_luid
        except AlreadyExistsException as e:
            self.log(u"Username '{}' already exists on the server; no updates performed".format(username))
            self.end_log_block()
            return e.existing_luid