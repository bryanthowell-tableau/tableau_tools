# -*- coding: utf-8 -*-

from tableau_exceptions import *
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


class TableauRepository:
    def __init__(self, tableau_server_url, repository_password, repository_username='readonly'):
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
    def query(self, sql, sql_parameter_array):

        cur = self.db_conn.cursor()
        cur.execute(sql, sql_parameter_array)
        return cur

    def query_sessions(self, username=None):
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
FROM sessions,
system_users,
users
WHERE sessions.user_id = users.id AND users.system_user_id = system_users.id
        """
        if username is not None:
            sessions_sql += "AND system_users.name = %s\n"
        sessions_sql += "ORDER BY sessions.updated_at DESC;"

        if username is not None:
            cur = self.query(sessions_sql, [username, ])
        else:
            cur = self.query(sessions_sql)
        return cur

    def query_subscriptions(self, schedule_name=None, views_only=True):
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