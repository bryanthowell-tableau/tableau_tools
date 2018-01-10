from ..tableau_exceptions import *


class UrlFilter:
    def __init__(self, field, operator, values):
        self.field = field
        self.operator = operator
        self.values = values

    def get_filter_string(self):
        """
        :rtype: unicode
        """
        if len(self.values) == 0:
            raise InvalidOptionException(u'Must pass in at least one value for the filter')
        elif len(self.values) == 1:
            value_string = self.values[0]
        else:
            value_string = u",".join(self.values)
            value_string = u"[{}]".format(value_string)
        url = u"{}:{}:{}".format(self.field, self.operator, value_string)
        return url


class UrlFilter23(UrlFilter):
    def __init__(self, field, operator, values):
        UrlFilter.__init__(self, field, operator, values)

    # Users, Datasources, Views, Workbooks
    @staticmethod
    def create_name_filter(name):
        """
        :type name: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'name', u'eq', [name, ])

    # Users
    @staticmethod
    def create_last_login_filter(operator, last_login_time):
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :type operator: unicode
        :param last_login_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        :type last_login_time: unicode
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'gt', u'gte', u'lt', u'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter(u'lastLogin', operator, [last_login_time, ])

    # Users
    @staticmethod
    def create_site_role_filter(site_role):
        """
        :type site_role: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'siteRole', u'eq', [site_role, ])

    # Workbooks
    @staticmethod
    def create_owner_name_filter(owner_name):
        """
        :type owner_name: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'ownerName', u'eq', [owner_name, ])

    # Workbooks, Datasources, Views
    @staticmethod
    def create_created_at_filter(operator, created_at_time):
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :type operator: unicode
        :param created_at_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        :type created_at_time: unicode
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'gt', u'gte', u'lt', u'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter(u'createdAt', operator, [created_at_time, ])

    # Workbooks, Datasources, Views
    @staticmethod
    def create_updated_at_filter(operator, updated_at_time):
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :type operator: unicode
        :param updated_at_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        :type updated_at_time: unicode
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'gt', u'gte', u'lt', u'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter(u'updatedAt', operator, [updated_at_time, ])

    # Workbooks, Datasources, Views
    @staticmethod
    def create_tags_filter(tags):
        """
        :type tags: list[unicode]
        :rtype: UrlFilter
        """
        return UrlFilter(u'tags', u'in', tags)

    # Workbooks, Datasources, Views
    @staticmethod
    def create_tag_filter(tag):
        """
        :type tag: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'tags', u'eq', [tag, ])


class UrlFilter24(UrlFilter23):
    def __init__(self, field, operator, values):
        UrlFilter23.__init__(self, field, operator, values)
    # Filtering added to Datasources and Views in 2.4

    # Datasources
    @staticmethod
    def create_datasource_type_filter(ds_type):
        """
        :type ds_type: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'type', u'eq', [ds_type, ])


class UrlFilter25(UrlFilter24):
    def __init__(self, field, operator, values):
        UrlFilter24.__init__(self, field, operator, values)
    # No changes were made in 2.5


class UrlFilter26(UrlFilter25):
    def __init__(self, field, operator, values):
        UrlFilter25.__init__(self, field, operator, values)
    # No changes in 2.6


class UrlFilter27(UrlFilter26):
    def __init__(self, field, operator, values):
        UrlFilter26.__init__(self, field, operator, values)
    # Some of the previous methods add in methods

    # Users, Datasources, Views, Workbooks
    @staticmethod
    def create_names_filter(names):
        """
        :type names: list[unicode]
        :rtype: UrlFilter
        """
        return UrlFilter(u'name', u'in', names)

    # Users
    @staticmethod
    def create_site_roles_filter(site_roles):
        """
        :type site_roles: list[unicode]
        :rtype: UrlFilter
        """
        return UrlFilter(u'siteRole', u'in', site_roles)

    # Workbooks, Projects
    @staticmethod
    def create_owner_names_filter(owner_names):
        """
        :type owner_names: list[unicode]
        :rtype: UrlFilter
        """
        return UrlFilter(u'ownerName', u'in', owner_names)

    # Workbooks. Datasources, Views
    # create_owner_name_filter (singular) allowed for all of them

    # Groups
    @staticmethod
    def create_domain_names_filter(domain_names):
        """
        :type domain_names: list[unicode]
        :rtype: UrlFilter
        """

        return UrlFilter(u'domainName', u'in', domain_names)

    # Groups
    @staticmethod
    def create_domain_nicknames_filter(domain_nicknames):
        """
        :type domain_nicknames: list[unicode]
        :rtype: UrlFilter
        """
        return UrlFilter(u'domainNickname', u'in', domain_nicknames)

    # Groups
    @staticmethod
    def create_domain_name_filter(domain_name):
        """
        :type domain_name: unicode
        :rtype: UrlFilter
        """

        return UrlFilter(u'domainName', u'eq', [domain_name, ])

    # Groups
    @staticmethod
    def create_domain_nickname_filter(domain_nickname):
        """
        :type domain_nickname: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'domainNickname', u'eq', [domain_nickname, ])

    # Groups
    @staticmethod
    def create_minimum_site_roles_filter(minimum_site_roles):
        """
        :type minimum_site_roles: list[unicode]
        :rtype: UrlFilter
        """
        return UrlFilter(u'minimumSiteRole', u'in', minimum_site_roles)

    # Groups
    @staticmethod
    def create_minimum_site_role_filter(minimum_site_role):
        """
        :type minimum_site_role: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'minimumSiteRole', u'eq', [minimum_site_role, ])

    # Groups
    @staticmethod
    def create_is_local_filter(is_local):
        """
        :type is_local:
        :return: bool
        """
        if is_local not in [True, False]:
            raise InvalidOptionException(u'is_local must be True or False')
        return UrlFilter(u'isLocal', u'eq', unicode(is_local).lower())

    # Groups
    @staticmethod
    def create_user_count_filter(operator, user_count):
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :type operator: unicode
        :type user_count: int
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'gt', u'gte', u'lt', u'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter(u'userCount', operator, [user_count, ])

    # Projects
    @staticmethod
    def create_owner_domains_filter(owner_domains):
        """
        :param owner_domains: list[unicode]
        :rtype: UrlFilter
        """
        return UrlFilter(u'ownerDomain', u'in', owner_domains)

    # Projects
    @staticmethod
    def create_owner_domain_filter(owner_domain):
        """
        :param owner_domain: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'ownerDomain', u'in', [owner_domain, ])

    # Projects
    @staticmethod
    def create_owner_emails_filter(owner_emails):
        """
        :param owner_emails: list[unicode]
        :rtype: UrlFilter
        """
        return UrlFilter(u'ownerEmail', u'in', owner_emails)

    # Projects
    @staticmethod
    def create_owner_email_filter(owner_email):
        """
        :param owner_email: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'ownerEmail', u'in', [owner_email, ])

    # Views
    @staticmethod
    def create_hits_total_filter(operator, hits_total):
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :type operator: unicode
        :type hits_total: int
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'gt', u'gte', u'lt', u'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter(u'hitsTotal', operator, [hits_total, ])


class UrlFilter28(UrlFilter27):
    def __init__(self, field, operator, values):
        UrlFilter27.__init__(self, field, operator, values)
    # No changes in 2.8
