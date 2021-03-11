from ..tableau_exceptions import *
from typing import Union, Optional, List, Dict, Tuple
import datetime

class UrlFilter:
    def __init__(self, field: str, operator: str, values: List[str]):
        self.field = field
        self.operator = operator
        self.values = values

    @staticmethod
    def datetime_to_tableau_date_str(dt: Union[str, datetime.datetime]) -> str:
        if isinstance(dt, datetime.datetime):
            return dt.isoformat('T')[:19] + 'Z'
        else:
            return dt


    def get_filter_string(self) -> str:
        if len(self.values) == 0:
            raise InvalidOptionException('Must pass in at least one value for the filter')
        elif len(self.values) == 1:
            value_string = self.values[0]
        else:
            value_string = ",".join(self.values)
            value_string = "[{}]".format(value_string)
        url = "{}:{}:{}".format(self.field, self.operator, value_string)
        return url

    # Users, Datasources, Views, Workbooks
    @staticmethod
    def get_name_filter(name: str) -> 'UrlFilter':
        return UrlFilter('name', 'eq', [name, ])

    # Users
    @staticmethod
    def get_last_login_filter(operator: str, last_login_time: Union[str, datetime.datetime]) -> 'UrlFilter':
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :param last_login_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        """
        comparison_operators = ['eq', 'gt', 'gte', 'lt', 'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException("operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format
        time = UrlFilter.datetime_to_tableau_date_str(last_login_time)

        return UrlFilter('lastLogin', operator, [time, ])

    # Users
    @staticmethod
    def get_site_role_filter(site_role: str) -> 'UrlFilter':
        return UrlFilter('siteRole', 'eq', [site_role, ])

    # Workbooks
    @staticmethod
    def get_owner_name_filter(owner_name: str) -> 'UrlFilter':

        return UrlFilter('ownerName', 'eq', [owner_name, ])

    # Workbooks, Datasources, Views, Jobs
    @staticmethod
    def get_created_at_filter(operator: str, created_at_time: Union[str, datetime.datetime]) -> 'UrlFilter':
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :param created_at_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        """
        comparison_operators = ['eq', 'gt', 'gte', 'lt', 'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException("operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format
        time = UrlFilter.datetime_to_tableau_date_str(created_at_time)
        return UrlFilter('createdAt', operator, [time, ])

    # Workbooks, Datasources, Views
    @staticmethod
    def get_updated_at_filter(operator: str, updated_at_time: Union[str, datetime.datetime]) -> 'UrlFilter':
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :param updated_at_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        """
        comparison_operators = ['eq', 'gt', 'gte', 'lt', 'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException("operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format
        time = updated_at_time

        return UrlFilter('updatedAt', operator, [time, ])

    # Workbooks, Datasources, Views
    @staticmethod
    def get_tags_filter(tags: List[str]) -> 'UrlFilter':
        return UrlFilter('tags', 'in', tags)

    # Workbooks, Datasources, Views
    @staticmethod
    def get_tag_filter(tag: str) -> 'UrlFilter':
        return UrlFilter('tags', 'eq', [tag, ])

    # Datasources
    @staticmethod
    def get_datasource_type_filter(ds_type: str) -> 'UrlFilter':
        return UrlFilter('type', 'eq', [ds_type, ])

class UrlFilter27(UrlFilter):
    def __init__(self, field, operator, values):
        UrlFilter.__init__(self, field, operator, values)
    # Some of the previous methods add in methods

    # Users, Datasources, Views, Workbooks
    @staticmethod
    def get_names_filter(names: List[str]) -> 'UrlFilter':
        return UrlFilter('name', 'in', names)

    # Users
    @staticmethod
    def get_site_roles_filter(site_roles: List[str]) -> 'UrlFilter':
        return UrlFilter('siteRole', 'in', site_roles)

    # Workbooks, Projects
    @staticmethod
    def get_owner_names_filter(owner_names: List[str]) -> 'UrlFilter':
        return UrlFilter('ownerName', 'in', owner_names)

    # Workbooks. Datasources, Views
    # get_owner_name_filter (singular) allowed for all of them

    # Groups
    @staticmethod
    def get_domain_names_filter(domain_names: List[str]) -> 'UrlFilter':
        return UrlFilter('domainName', 'in', domain_names)

    # Groups
    @staticmethod
    def get_domain_nicknames_filter(domain_nicknames: List[str]) -> 'UrlFilter':

        return UrlFilter('domainNickname', 'in', domain_nicknames)

    # Groups
    @staticmethod
    def get_domain_name_filter(domain_name: str) -> 'UrlFilter':
        return UrlFilter('domainName', 'eq', [domain_name, ])

    # Groups
    @staticmethod
    def get_domain_nickname_filter(domain_nickname: str) -> 'UrlFilter':

        return UrlFilter('domainNickname', 'eq', [domain_nickname, ])

    # Groups
    @staticmethod
    def get_minimum_site_roles_filter(minimum_site_roles: List[str]) -> 'UrlFilter':
        return UrlFilter('minimumSiteRole', 'in', minimum_site_roles)

    # Groups
    @staticmethod
    def get_minimum_site_role_filter(minimum_site_role: str) -> 'UrlFilter':
        return UrlFilter('minimumSiteRole', 'eq', [minimum_site_role, ])

    # Groups
    @staticmethod
    def get_is_local_filter(is_local: bool) -> 'UrlFilter':
        if is_local not in [True, False]:
            raise InvalidOptionException('is_local must be True or False')
        return UrlFilter('isLocal', 'eq', [str(is_local).lower(), ])

    # Groups
    @staticmethod
    def get_user_count_filter(operator, user_count: int) -> 'UrlFilter':
        comparison_operators = ['eq', 'gt', 'gte', 'lt', 'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException("operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")

        return UrlFilter('userCount', operator, [str(user_count), ])

    # Projects
    @staticmethod
    def get_owner_domains_filter(owner_domains: List[str]) -> 'UrlFilter':
        return UrlFilter('ownerDomain', 'in', owner_domains)

    # Projects
    @staticmethod
    def get_owner_domain_filter(owner_domain: str) -> 'UrlFilter':
        return UrlFilter('ownerDomain', 'in', [owner_domain, ])

    # Projects
    @staticmethod
    def get_owner_emails_filter(owner_emails: List[str]) -> 'UrlFilter':
        return UrlFilter('ownerEmail', 'in', owner_emails)

    # Projects
    @staticmethod
    def get_owner_email_filter(owner_email: str) -> 'UrlFilter':
        return UrlFilter('ownerEmail', 'in', [owner_email, ])

    # Views
    @staticmethod
    def get_hits_total_filter(operator, hits_total: int) -> 'UrlFilter':
        comparison_operators = ['eq', 'gt', 'gte', 'lt', 'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException("operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter('hitsTotal', operator, [str(hits_total), ])


class UrlFilter28(UrlFilter27):
    def __init__(self, field, operator, values):
        UrlFilter27.__init__(self, field, operator, values)
    # No changes in 2.8


class UrlFilter30(UrlFilter28):
    def __init__(self, field, operator, values):
        UrlFilter28.__init__(self, field, operator, values)
    # No changes in 3.0


class UrlFilter31(UrlFilter30):
    def __init__(self, field, operator, values):
        UrlFilter30.__init__(self, field, operator, values)

    # Jobs
    @staticmethod
    def get_started_at_filter(operator: str, started_at_time: str) -> UrlFilter:
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :param started_at_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        """
        comparison_operators = ['eq', 'gt', 'gte', 'lt', 'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException("operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format
        time = UrlFilter.datetime_to_tableau_date_str(started_at_time)
        return UrlFilter('createdAt', operator, [time, ])

    # Jobs
    @staticmethod
    def get_ended_at_filter(operator: str, ended_at_time: str) -> UrlFilter:
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :param ended_at_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        """
        comparison_operators = ['eq', 'gt', 'gte', 'lt', 'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException("operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format
        time = UrlFilter.datetime_to_tableau_date_str(ended_at_time)
        return UrlFilter('createdAt', operator, [time, ])

    # Jobs
    @staticmethod
    def get_job_types_filter(job_types: List[str]) -> UrlFilter:
        return UrlFilter('jobType', 'in', job_types)

    # Jobs
    @staticmethod
    def get_job_type_filter(job_type: str) -> UrlFilter:
        return UrlFilter('tags', 'eq', [job_type, ])

    # Jobs
    @staticmethod
    def get_notes_filter(notes: str) -> UrlFilter:
        return UrlFilter('notes', 'has', [notes, ])

    @staticmethod
    def get_title_equals_filter(title: str) -> UrlFilter:
        return UrlFilter('title', 'eq', [title, ])

    @staticmethod
    def get_title_has_filter(title: str) -> UrlFilter:
        return UrlFilter('title', 'has', [title, ])

    @staticmethod
    def get_subtitle_equals_filter(subtitle: str) -> UrlFilter:
        return UrlFilter('subtitle', 'eq', [subtitle, ])

    @staticmethod
    def get_subtitle_has_filter(subtitle: str) -> UrlFilter:
        return UrlFilter('subtitle', 'has', [subtitle, ])

class UrlFilter33(UrlFilter31):
    def __init__(self, field, operator, values):
        UrlFilter31.__init__(self, field, operator, values)

    @staticmethod
    def get_project_name_equals_filter(project_name: str) -> UrlFilter:
        return UrlFilter('projectName', 'eq', [project_name, ])
