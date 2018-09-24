from tableau_rest_api_connection_26 import *


class TableauRestApiConnection27(TableauRestApiConnection26):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection26.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"10.4")

    def update_datasource(self, datasource_name_or_luid, datasource_project_name_or_luid=None,
                          new_datasource_name=None, new_project_luid=None, new_owner_luid=None,
                          certification_status=None, certification_note=None):
        """
        :type datasource_name_or_luid: unicode
        :type datasource_project_name_or_luid: unicode
        :type new_datasource_name: unicode
        :type new_project_luid: unicode
        :type new_owner_luid: unicode
        :type certification_status: bool
        :type certification_note: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if certification_status not in [None, False, True]:
            raise InvalidOptionException(u'certification_status must be None, False, or True')

        if self.is_luid(datasource_name_or_luid):
            datasource_luid = datasource_name_or_luid
        else:
            datasource_luid = self.query_datasource_luid(datasource_name_or_luid, datasource_project_name_or_luid)

        tsr = etree.Element(u"tsRequest")
        d = etree.Element(u"datasource")
        if new_datasource_name is not None:
            d.set(u'name', new_datasource_name)
        if certification_status is not None:
            d.set(u'isCertified', u'{}'.format(unicode(certification_status).lower()))
        if certification_note is not None:
            d.set(u'certificationNote', certification_note)
        if new_project_luid is not None:
            p = etree.Element(u'project')
            p.set(u'id', new_project_luid)
            d.append(p)
        if new_owner_luid is not None:
            o = etree.Element(u'owner')
            o.set(u'id', new_owner_luid)
            d.append(o)

        tsr.append(d)

        url = self.build_api_url(u"datasources/{}".format(datasource_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

        #
        # Start Group Query Methods
        #

    def query_groups(self, name_filter=None, domain_name_filter=None, domain_nickname_filter=None, is_local_filter=None,
                     user_count_filter=None, minimum_site_role_filter=None, sorts=None):
        """
        :type name_filter: UrlFilter
        :type domain_name_filter: UrlFilter
        :type domain_nickname_filter: UrlFilter
        :type is_local_filter: UrlFilter
        :type user_count_filter: UrlFilter
        :type minimum_site_role_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        filter_checks = {u'name': name_filter, u'domainName': domain_name_filter,
                         u'domainNickname': domain_nickname_filter, u'isLocal': is_local_filter,
                         u'userCount': user_count_filter, u'minimumSiteRole': minimum_site_role_filter}

        filters = self._check_filter_objects(filter_checks)

        self.start_log_block()
        groups = self.query_resource(u"groups", filters=filters, sorts=sorts)
        for group in groups:
            # Add to group-name : luid cache
            group_luid = group.get(u"id")
            group_name = group.get(u'name')
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return groups

        # # No basic verb for querying a single group, so run a query_groups

    def query_group(self, group_name_or_luid):
        """
        :type group_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        group = self.query_single_element_from_endpoint_with_filter(u'group', group_name_or_luid)
        # Add to group_name : luid cache
        group_luid = group.get(u"id")
        group_name = group.get(u'name')
        self.group_name_luid_cache[group_name] = group_luid

        self.end_log_block()
        return group

        # Groups luckily cannot have the same 'pretty name' on one site

    def query_group_luid(self, group_name):
        """
        :type group_name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if group_name in self.group_name_luid_cache:
            group_luid = self.group_name_luid_cache[group_name]
            self.log(u'Found group name {} in cache with luid {}'.format(group_name, group_luid))
        else:
            group_luid = self.query_single_element_luid_from_endpoint_with_filter(u'group', group_name)
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return group_luid

    #
    # End Group Querying methods
    #

    #
    # Start Project Querying methods
    #

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
        filter_checks = {u'name': name_filter, u'ownerName': owner_name_filter,
                         u'updatedAt': updated_at_filter, u'createdAt': created_at_filter,
                         u'ownerDomain': owner_domain_filter, u'ownerEmail': owner_email_filter}

        filters = self._check_filter_objects(filter_checks)

        self.start_log_block()
        projects = self.query_resource(u"projects", filters=filters, sorts=sorts)
        self.end_log_block()
        return projects

    def query_project_luid(self, project_name):
        """
        :type project_name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        project_luid = self.query_single_element_luid_from_endpoint_with_filter(u'project', project_name)
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
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint_with_filter(u'project',
                                                                                               project_name_or_luid))

        self.end_log_block()
        return proj

    #
    # End Project Querying Methods
    #