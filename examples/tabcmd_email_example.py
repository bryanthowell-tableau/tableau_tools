# -*- coding: utf-8 -*-
from tableau_tools import *

tableau_server_admin_user = ''
tableau_server_admin_pw = ''
tableau_server_url = 'http://127.0.0.1'
tabcmd_dir = "C:\\tabcmd\\Command Line Utility\\"
tabcmd_config_location = 'C:\\Users\\{user}\\AppData\\Local\\Tableau\\Tabcmd\\'
tabcmd_repository_pw = ''
smtp_server = 'email.somewhere.lan'

schedule_name_to_keep = "-- Full PDF Every Morning"

tableau_emailer = TableauEmailer(tabcmd_dir, tabcmd_config_location, tabcmd_repository_pw, tableau_server_url,
                                 tableau_server_admin_user, tableau_server_admin_pw, smtp_server)
tableau_emailer.tabcmd.export_type = 'fullpdf'
tableau_emailer.tabcmd.export_page_layout = 'landscape'
tableau_emailer.generate_emails_from_named_schedule_in_repository(schedule_name_to_keep, 'noreply@tableau.com',
                                                                  'basic_email')

# tableau_emailer.generate_email_from_view('donotreply@tableau.com', 'someone@somewhere.com', email_subject,
#                                            'basic_email', user, view_location)

tableau_emailer.tabcmd.site = 'default'
for region in ['East', 'West', 'Central', 'South']:
    print "Bulding the {} e-mail".format(region)
    tableau_emailer.generate_email_from_view('donotreply@somewhere.com', 'someone@somewhere.com',
                                             'This viz is the {}!'.format(region), 'basic_email', 'bhowell',
                                             'Book1/Dashboard1', view_filter_map={"Region": region},
                                             email_content_type='csv')

#tableau_emailer.generate_email_from_view('donotreply@somewhere.com', 'someone@somewhere.com',
#                                             'This viz has two regions!', 'basic_email', 'bhowell',
#                                             'Book1/Dashboard1', {"Region": ['East', 'West']})



