import json
import urllib
import xml.etree.ElementTree as ET

from tableau_rest_api.tableau_rest_api_connection import *


class Tabcmd:
    def __init__(self, tabcmd_folder, tableau_server_url, username, password, site='default',
                 repository_password=None, tabcmd_config_location=None):
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
        self.create_tabcmd_admin_session()
    #
    # Wrapper commands for Tabcmd command line actions
    #

    def get_directory_cmd(self):
        return 'cd "{}" '.format(self.tabcmd_folder)

    def get_login_cmd(self, pw_filename):

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

    def get_export_cmd(self, export_type, filename, view_url, view_filter_map=None, refresh=False):
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

    #
    # Methods for Creating TabCmd Session for the appropriate user
    #

    def create_tabcmd_admin_session(self):
        # Create a password file so the password doesn't run in the logs / command line
        pw_filename = self.tabcmd_folder + 'dorwsasp.txt'
        login_cmds = self.get_login_cmd(pw_filename)
        directory_cmd = self.get_directory_cmd()
        temp_bat = open('login.bat', 'w')

        temp_bat.write(directory_cmd + "\n")
        temp_bat.write(login_cmds + "\n")
        temp_bat.close()

        os.system("login.bat")
        os.remove("login.bat")
        # Kill the password file as soon as it has run.
        os.remove(pw_filename)

    def set_tabcmd_auth_info_from_repository_for_impersonation(self, username_to_impersonate):

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

    def configure_tabcmd_config_for_user_session(self, user):
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

    def create_session_and_configure_tabcmd_for_user(self, user, view_location):
        self.get_trusted_ticket_for_user(user)
        self.redeem_trusted_ticket(view_location)
        self.set_tabcmd_auth_info_from_repository_for_impersonation(user)
        self.configure_tabcmd_config_for_user_session(user)

    def create_export(self, export_type, view_location, view_filter_map=None, user_to_impersonate=None,
                      filename='tableau_workbook'):
        if self.export_type is not None:
            export_type = self.export_type
        if export_type.lower() not in ['pdf', 'csv', 'png', 'fullpdf']:
            raise Exception(msg='Options are pdf fullpdf csv or png')
        #
        if user_to_impersonate is not None:
            self.create_session_and_configure_tabcmd_for_user(user_to_impersonate, view_location)

        directory_cmd = self.get_directory_cmd()
        # fullpdf still ends with pdf
        if export_type.lower() == 'fullpdf':
            saved_filename = '{}.{}'.format(filename, 'pdf')
        else:
            saved_filename = '{}.{}'.format(filename, export_type.lower())

        export_cmds = self.get_export_cmd(export_type, saved_filename, view_location, view_filter_map)

        temp_bat = open('export.bat', 'w')

        temp_bat.write(directory_cmd + "\n")
        temp_bat.write(export_cmds + "\n")
        temp_bat.close()

        os.system("export.bat")
        os.remove("export.bat")
        full_file_location = self.tabcmd_folder + saved_filename
        return full_file_location