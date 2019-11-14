# -*- coding: utf-8 -*-

# Import smtplib for the actual sending function
import smtplib
# Import the email modules we'll need
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import Encoders
from email.mime.base import MIMEBase

import os
from os.path import basename
from .tableau_repository import TableauRepository
from .tabcmd import Tabcmd


class TableauEmailer:
    def __init__(self, tabcmd_dir, tabcmd_config_location, repository_pw,
                 tableau_server_url, tableau_server_admin_user, tableau_server_admin_pw,
                 smtp_server, smtp_username=None, smtp_password=None):

        self.tableau_server_url = tableau_server_url
        self.repository_pw = repository_pw
        self.smtp_server = smtplib.SMTP(smtp_server)
        if smtp_username is not None and smtp_password is not None:
            self.smtp_server.login(smtp_username, smtp_password)

        # Create the tabcmd obj so you can do some querying. We will change the site later on at will
        self.tabcmd = Tabcmd(tabcmd_dir, tableau_server_url, tableau_server_admin_user, tableau_server_admin_pw,
                             repository_password=repository_pw, tabcmd_config_location=tabcmd_config_location)

    def email_file_from_template(self, from_user, to_user, subject, template_name, filename_to_attach):
        # Every template_name should have two versions, a .html and a .txt fallback
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = from_user
        msg['To'] = to_user
        text_fh = open(template_name + '.txt')
        html_fh = open(template_name + '.html')
        part1 = MIMEText(text_fh.read(), 'plain')
        part2 = MIMEText(html_fh.read(), 'html')
        msg.attach(part1)
        msg.attach(part2)
        text_fh.close()
        html_fh.close()
        filename_to_attach = filename_to_attach.replace("\\", "/")

        base_filename = basename(filename_to_attach)
        file_extension = os.path.splitext(base_filename)

        with open(filename_to_attach, 'rb') as attach_fh:

            file_to_attach = MIMEBase('application', file_extension)
            file_to_attach.set_payload(attach_fh.read())
            Encoders.encode_base64(file_to_attach)
            file_to_attach.add_header('Content-Disposition', 'attachment', filename=base_filename)
            msg.attach(file_to_attach)
        self.smtp_server.sendmail(from_user, to_user, msg.as_string())
        # Cleanup the file
        os.remove(filename_to_attach)

    def generate_email_from_view(self, email_from_user, email_to_user, email_subject, email_template_name,
                                 view_user, view_location, email_content_type='fullpdf', view_filter_map=None):
        filename_to_attach = self.tabcmd.create_export(email_content_type, view_location, user_to_impersonate=view_user,
                                                       view_filter_map=view_filter_map)
        self.email_file_from_template(email_from_user, email_to_user, email_subject, email_template_name,
                                      filename_to_attach)

    def generate_emails_from_named_schedule_in_repository(self, schedule_name, email_from_user, email_template_name,
                                                          email_content_type='fullpdf'):
        repository = TableauRepository(self.tableau_server_url, self.repository_pw)
        cur = repository.query_subscriptions(schedule_name)
        for row in cur:
            email_subject = row[1]
            user = row[2]
            site = row[3]
            view_location = row[4]
            # schedule_name = row[5]
            user_email = row[6]
            # Configure the site
            self.tabcmd.site = site
            self.generate_email_from_view(email_from_user, user_email, email_subject, email_template_name, user,
                                          view_location, email_content_type=email_content_type)