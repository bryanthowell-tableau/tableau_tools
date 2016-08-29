import json
import urllib
import xml.etree.ElementTree as ET

from tableau_rest_api.tableau_rest_api_connection import *
from tableau_tools.tableau_repository import *
from tableau_tools.tableau_http import *
from tableau_tools.tableau_base import TableauBase


class Tabcmd(TableauBase):
    def __init__(self, tabcmd_folder, tableau_server_url, username, password, site='default',
                 repository_password=None, tabcmd_config_location=None):
        super(self.__class__, self).__init__()
        self.logger = None

        self.tabcmd_folder = tabcmd_folder
        self.username = username
        self.password = password
        self.site = site
        self.tableau_server_url = tableau_server_url
        self.current_trusted_ticket = None

        # user 'tableau' does not have enough rights
        self.repository_pw = repository_password
        self.user_session_id = None
        self.user_auth_token = None
        self.tabcmd_config_location = tabcmd_config_location
        self.tabcmd_config_filename = 'tabcmd-session.xml'

        # Configurable options for the exports
        self.export_pagesize = 'letter'
        self.export_page_layout = 'portrait'
        self.export_full_pdf = False
        self.export_width_pixels = 800
        self.export_height_pixels = 600
        self.export_type = None

        # Go ahead and prep for any subsequent calls
        self._create_tabcmd_admin_session()

    #
    # Wrapper commands for Tabcmd command line actions
    #

    def build_directory_cmd(self):
        return 'cd "{}" '.format(self.tabcmd_folder)

    def build_login_cmd(self, pw_filename):

        pw_file = open(pw_filename, 'w')
        pw_file.write(self.password)
        pw_file.close()
        if self.site.lower() == 'default':
            cmd = "tabcmd login -s {} -u {} --password-file \"{}\"".format(self.tableau_server_url,
                                                                           self.username, pw_filename)
        else:
            cmd = "tabcmd login -s {} -t {} -u {} --password-file \"{}\"".format(self.tableau_server_url, self.site,
                                                                                 self.username, pw_filename)

        return cmd

    def build_export_cmd(self, export_type, filename, view_url, view_filter_map=None, refresh=False):
        # view_filter_map allows for passing URL filters or parameters
        if export_type.lower() not in ['pdf', 'csv', 'png', 'fullpdf']:
            raise Exception(msg='Should be pdf fullpdf csv or png')
        additional_url_params = ""
        if view_filter_map is not None:
            additional_url_params = "?" + urllib.urlencode(view_filter_map)
            if refresh is True:
                additional_url_params += "&:refresh"
        elif view_filter_map is None:
            if refresh is True:
                additional_url_params += "?:refresh"
        view_url += additional_url_params

        cmd = 'tabcmd export "{}" --filename "{}" --{} --pagelayout {} --pagesize {} --width {} --height {}'.format(
            view_url, filename, export_type, self.export_page_layout, self.export_pagesize, self.export_width_pixels,
            self.export_height_pixels
        )

        return cmd

    @staticmethod
    def build_refreshextracts_cmd(project, workbook_or_datasource, content_pretty_name,
                                incremental=False, workbook_url_name=None):
        project_cmd = '--project "{}"'.format(project)
        if project.lower() == u'default':
            project_cmd = ''

        inc_cmd = ''
        if incremental is True:
            inc_cmd = '--incremental'

        if workbook_url_name is not None:
            content_cmd = '--url {}'.format(workbook_url_name)
        else:
            if workbook_or_datasource.lower() == 'workbook':
                content_cmd = '--workbook "{}"'.format(content_pretty_name)
            elif workbook_or_datasource.lower() == 'datasource':
                content_cmd = '--datasource "{}"'.format(content_pretty_name)
            else:
                raise InvalidOptionException('workbook_or_datasource must be either workbook or datasource')

        cmd = 'tabcmd refreshextracts {} {} {}'.format(project_cmd, content_cmd, inc_cmd)
        return cmd

    @staticmethod
    def build_runschedule_cmd(schedule_name):
        cmd = 'tabcmd runschedule "{}"'.format(schedule_name)
        return cmd

    #
    # Methods for Creating TabCmd Session for the appropriate user
    #

    def _create_tabcmd_admin_session(self):
        # Create a password file so the password doesn't run in the logs / command line
        pw_filename = self.tabcmd_folder + 'dorwsasp.txt'
        login_cmds = self.build_login_cmd(pw_filename)
        directory_cmd = self.build_directory_cmd()
        temp_bat = open('login.bat', 'w')

        temp_bat.write(directory_cmd + "\n")
        temp_bat.write(login_cmds + "\n")
        temp_bat.close()

        os.system("login.bat")
        os.remove("login.bat")
        # Kill the password file as soon as it has run.
        os.remove(pw_filename)

    def _set_tabcmd_auth_info_from_repository_for_impersonation(self, username_to_impersonate):
        # After you create a session, you must query the repository to retrieve the auth_token from it
        repository = TableauRepository(self.tableau_server_url, self.repository_pw)
        cur = repository.query_sessions(username_to_impersonate)

        # Did anything return?
        if cur.rowcount > 0:
            first_row = cur.fetchone()
            self.user_session_id = first_row[0]

            wg_json = first_row[4]
            json_obj = json.loads(wg_json)
            self.user_auth_token = json_obj["auth_token"]
            cur.close()
        else:
            raise NoResultsException('There were no sessions found for the username {}'.format(username_to_impersonate))

    def _configure_tabcmd_config_for_user_session(self, user):
        # tabcmd keeps a session history, stored within its XML configuration file.
        # Rather than logging into tabcmd again, once there is a session history, we simply substitute in the
        # impersonated user's info directly into the XML.
        xml_tree = ET.parse(self.tabcmd_config_location + self.tabcmd_config_filename)
        root = xml_tree.getroot()

        for child in root:
            if child.tag == 'username':
                child.text = user
            if child.tag == 'base-url':
                child.text = self.tableau_server_url
            if child.tag == 'session-id':
                child.text = self.user_session_id
            if child.tag == 'authenticity-token':
                child.text = self.user_auth_token
            if child.tag == 'site-prefix':
                if self.site.lower() != 'default':
                    child.text = 't/{}'.format(self.site)
                else:
                    child.text = None
        xml_tree.write(self.tabcmd_config_location + self.tabcmd_config_filename, encoding='UTF8',
                       xml_declaration=True, default_namespace=None
                       )

    def _create_session_and_configure_tabcmd_for_user(self, user, view_location):
        tabhttp = TableauHTTP(self.tableau_server_url)
        tabhttp.create_trusted_ticket_session(view_location, user, site=self.site)
        self._set_tabcmd_auth_info_from_repository_for_impersonation(user)
        self._configure_tabcmd_config_for_user_session(user)

    #
    # Methods to use
    #
    def create_export(self, export_type, view_location, view_filter_map=None, user_to_impersonate=None,
                      filename='tableau_workbook'):
        self.start_log_block()
        if self.export_type is not None:
            export_type = self.export_type
        if export_type.lower() not in ['pdf', 'csv', 'png', 'fullpdf']:
            raise Exception(msg='Options are pdf fullpdf csv or png')
        #
        if user_to_impersonate is not None:
            self._create_session_and_configure_tabcmd_for_user(user_to_impersonate, view_location)

        directory_cmd = self.build_directory_cmd()
        # fullpdf still ends with pdf
        if export_type.lower() == 'fullpdf':
            saved_filename = '{}.{}'.format(filename, 'pdf')
        else:
            saved_filename = '{}.{}'.format(filename, export_type.lower())

        export_cmds = self.build_export_cmd(export_type, saved_filename, view_location, view_filter_map)

        temp_bat = open('export.bat', 'w')

        temp_bat.write(directory_cmd + "\n")
        temp_bat.write(export_cmds + "\n")
        temp_bat.close()

        os.system("export.bat")
        os.remove("export.bat")
        full_file_location = self.tabcmd_folder + saved_filename
        self.end_log_block()
        return full_file_location

    def trigger_extract_refresh(self, project, workbook_or_datasource, content_pretty_name, incremental=False,
                                workbook_url_name=None):
        self.start_log_block()
        refresh_cmd = self.build_refreshextracts_cmd(project, workbook_or_datasource, content_pretty_name, incremental,
                                                     workbook_url_name=workbook_url_name)
        temp_bat = open('refresh.bat', 'w')
        temp_bat.write(self.build_directory_cmd() + "\n")
        temp_bat.write(refresh_cmd + "\n")
        temp_bat.close()

        os.system("refresh.bat")
        os.remove("refresh.bat")
        self.end_log_block()

    def trigger_schedule_run(self, schedule_name):
        self.start_log_block()
        cmd = self.build_runschedule_cmd(schedule_name)

        temp_bat = open('runschedule.bat', 'w')
        temp_bat.write(self.build_directory_cmd() + "\n")
        temp_bat.write(cmd + "\n")
        temp_bat.close()

        os.system("runschedule.bat")
        os.remove("runschedule.bat")
        self.end_log_block()
