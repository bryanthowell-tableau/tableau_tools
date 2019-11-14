# -*- coding: utf-8 -*-

from .tableau_exceptions import *
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
from typing import Union, Any, Optional, List, Dict, Tuple

class TableauRepository:
    def __init__(self, tableau_server_url: str, repository_password: str, repository_username: str = 'readonly'):
        if repository_username not in ['tableau', 'readonly', 'tblwgadmin']:
            raise InvalidOptionException('Must use one of the three valid usernames')

        # Remove the http:// or https:// to log in to the repository. (Do we need things if this is SSL?)
        colon_slash_slash = tableau_server_url.find('://')
        if colon_slash_slash != -1:
            self.repository_server = tableau_server_url[colon_slash_slash+2:]
        else:
            self.repository_server = tableau_server_url

        self.repository_port = 8060
        self.repository_db = 'workgroup'
        # user 'tableau' does not have enough rights
        self.repository_user = repository_username
        self.repository_pw = repository_password

        self.db_conn = psycopg2.connect(host=self.repository_server, database=self.repository_db,
                                        user=self.repository_user, password=self.repository_pw,
                                        port=self.repository_port)
        self.db_conn.set_session(autocommit=True)

    def __del__(self):
        self.db_conn.close()

    # Base method for querying
    def query(self, sql: str, sql_parameter_list: Optional[List] = None):

        cur = self.db_conn.cursor()
        if sql_parameter_list is not None:
            cur.execute(sql, sql_parameter_list)
        else:
            cur.execute(sql)
        return cur

    def query_sessions(self, username: Optional[str] = None):
        # Trusted tickets sessions do not have anything in the 'data' column
        # The auth token is contained within the shared_wg_write column, stored as JSON
        sessions_sql = """
SELECT
sessions.session_id,
sessions.data,
sessions.updated_at,
sessions.user_id,
sessions.shared_wg_write,
sessions.shared_vizql_write,
system_users.name AS user_name,
users.system_user_id
FROM sessions
JOIN users ON sessions.user_id = users.id
JOIN system_users ON users.system_user_id = system_users.id
"""
        if username is not None:
            sessions_sql += "WHERE system_users.name = %s\n"
        sessions_sql += "ORDER BY sessions.updated_at DESC;"

        if username is not None:
            cur = self.query(sessions_sql, [username, ])
        else:
            cur = self.query(sessions_sql)
        return cur

    def query_subscriptions(self, schedule_name: Optional[str] = None, views_only: bool = True):
        subscriptions_sql = """
SELECT
s.id,
s.subject,
s.user_name,
s.site_name,
COALESCE(cv.repository_url, s.view_url) as view_url,
sch.name,
su.email
FROM _subscriptions s
LEFT JOIN _customized_views cv  ON s.customized_view_id = cv.id
JOIN _schedules sch ON sch.name = s.schedule_name
JOIN system_users su ON su.name = s.user_name
"""
        if schedule_name is not None:
            subscriptions_sql += 'WHERE sch.name = %s\n'
            if views_only is True:
                subscriptions_sql += 'AND s.view_url IS NOT NULL -- Export command in tabcmd requires a View not a Workbook'
        else:
            if views_only is True:
                subscriptions_sql += 'WHERE s.view_url IS NOT NULL -- Export command in tabcmd requires a View not a Workbook'

        if schedule_name is not None:
            cur = self.query(subscriptions_sql, [schedule_name, ])
        else:
            cur = self.query(subscriptions_sql)
        return cur

    # Set extract refresh schedules
    def query_extract_schedules(self, schedule_name: Optional[str] = None):
        schedules_sql = """
SELECT *
FROM _schedules
WHERE scheduled_action_type = 'Refresh Extracts'
AND hidden = false
"""
        if schedule_name is not None:
            schedules_sql += 'AND name = %s\n'
            cur = self.query(schedules_sql, [schedule_name, ])
        else:
            cur = self.query(schedules_sql)
        return cur

    def get_extract_schedule_id_by_name(self, schedule_name: str):
        cur = self.query_extract_schedules(schedule_name=schedule_name)
        if cur.rowcount == 0:
            raise NoMatchFoundException('No schedule found with name "{}"'.format(schedule_name))
        sched_id = None
        # Should only be one row
        for row in cur:
            sched_id = row[0]
        return sched_id

    def query_sites(self, site_content_url: Optional[str] = None, site_pretty_name: Optional[str] = None):
        if site_content_url is None and site_pretty_name is None:
            raise InvalidOptionException('Must pass one of either the site_content_url or site_pretty_name')

        sites_sql = """
SELECT *
FROM _sites
"""
        if site_content_url is not None and site_pretty_name is None:
            sites_sql += 'WHERE url_namespace = %s\n'
            cur = self.query(sites_sql, [site_content_url, ])
        elif site_content_url is None and site_pretty_name is not None:
            sites_sql += 'WHERE name = %s\n'
            cur = self.query(sites_sql, [site_pretty_name, ])
        else:
            sites_sql += 'WHERE url_namesspace = %s AND name = %s\n'
            cur = self.query(sites_sql, [site_content_url, site_pretty_name])

        return cur

    def get_site_id_by_site_content_url(self, site_content_url: str):
        cur = self.query_sites(site_content_url=site_content_url)
        if cur.rowcount == 0:
            raise NoMatchFoundException('No site found with content url "{}"'.format(site_content_url))
        site_id = None
        # Should only be one row
        for row in cur:
            site_id = row[0]
        return site_id

    def get_site_id_by_site_pretty_name(self, site_pretty_name: str):
        cur = self.query_sites(site_pretty_name=site_pretty_name)
        if cur.rowcount == 0:
            raise NoMatchFoundException('No site found with pretty name "{}"'.format(site_pretty_name))
        site_id = None
        # Should only be one row
        for row in cur:
            site_id = row[0]
        return site_id

    def query_project_id_on_site_by_name(self, project_name: str, site_id: str):
        project_sql = """
        SELECT *
        FROM _projects
        WHERE project_name = %s
        AND site_id = %s
"""
        cur = self.query(project_sql, [project_name, site_id])
        if cur.rowcount == 0:
            raise NoMatchFoundException('No project named {} found on the site'.format(project_name))
        project_id = None
        for row in cur:
            project_id = row[0]
        return project_id

    def query_datasource_id_on_site_in_project(self, datasource_name: str, site_id: str, project_id: str):
        datasource_query = """
        SELECT id
        FROM _datasources
        WHERE name = %s
        AND site_id = %s
        AND project_id = %s
"""
        cur = self.query(datasource_query, [datasource_name, site_id, project_id])
        if cur.rowcount == 0:
            raise NoMatchFoundException('No data source found with name "{}"'.format(datasource_name))
        datasource_id = None
        for row in cur:
            datasource_id = row[0]
        return datasource_id

    def query_workbook_id_on_site_in_project(self, workbook_name: str, site_id: str, project_id: str):
        workbook_query = """
        SELECT id
        FROM _workbooks
        WHERE name = %s
        AND site_id = %s
        AND project_id = %s
"""
        cur = self.query(workbook_query, [workbook_name, site_id, project_id])
        if cur.rowcount == 0:
            raise NoMatchFoundException('No workbook found with name "{}"'.format(workbook_name))
        workbook_id = None
        for row in cur:
            workbook_id = row[0]
        return workbook_id

    def query_workbook_id_from_luid(self, workbook_luid: str):
        workbook_query = """
        SELECT id
        FROM workbooks
        WHERE luid = %s
"""
        cur = self.query(workbook_query, [workbook_luid, ])
        if cur.rowcount == 0:
            raise NoMatchFoundException('No workbook found with luid "{}"'.format(workbook_luid))
        workbook_id = None
        for row in cur:
            workbook_id = row[0]
        return workbook_id

    def query_site_id_from_workbook_luid(self, workbook_luid: str):
        workbook_query = """
            SELECT site_id
            FROM workbooks
            WHERE luid = %s
    """
        cur = self.query(workbook_query, [workbook_luid, ])
        if cur.rowcount == 0:
            raise NoMatchFoundException('No workbook found with luid "{}"'.format(workbook_luid))
        workbook_id = None
        for row in cur:
            workbook_id = row[0]
        return workbook_id

    def query_datasource_id_from_luid(self, datasource_luid: str):
        datasource_query = """
        SELECT id
        FROM datasources
        WHERE luid = %s
"""
        cur = self.query(datasource_query, [datasource_luid, ])
        if cur.rowcount == 0:
            raise NoMatchFoundException('No data source found with luid "{}"'.format(datasource_luid))
        datasource_id = None
        for row in cur:
            datasource_id = row[0]
        return datasource_id

    def query_site_id_from_datasource_luid(self, datasource_luid: str):
        datasource_query = """
        SELECT site_id
        FROM datasources
        WHERE luid = %s
"""
        cur = self.query(datasource_query, [datasource_luid, ])
        if cur.rowcount == 0:
            raise NoMatchFoundException('No data source found with luid "{}"'.format(datasource_luid))
        datasource_id = None
        for row in cur:
            datasource_id = row[0]
        return datasource_id

# Need to add in some classes to find Custom View LUIDs
