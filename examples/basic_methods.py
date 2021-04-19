# -*- coding: utf-8 -*-

from tableau_tools import *

# This example shows how to build up new commands from the basic components, to implement new features or arguments or
# whatever in the future

server = 'http://127.0.0.1'
username = ''
password = ''

# Using api_version overrides the default internal API version that would be used by the class. It would have been 3.6, b
# but the optional argument is forcing the commands to be sent with 3.11 instead of 3.6 in the API URLs
d = TableauServerRest36(server=server, username=username, password=password, site_content_url='default', api_version='3.11')
d.signin()

# Maybe "Get Recently Viewed for Site" hasn't been implemented yet or was forgotten
# https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_site.htm#get_recently_viewed
# Since it is a get command, we can use query_resource

recently_viewed = d.query_resource("content/recent")
data_acceleration_report = d.query_resource('dataAccelerationReport')

# What if there were additional properties to a Site update that the current update_site() doesn't handle

# server_level=True excludes the "site/site-id/" portion that would typically be automatically added
url = d.build_api_url("", server_level=True)

# build the XML request
# All requests start with a 'tsRequest' outer element
tsr = ET.Element('tsRequest')
s = ET.Element('site')
s.set('name', 'Fancy New Site Name')
s.set('state', 'Disabled')
s.set('catalogObfuscationEnabled','true')

# Append all your inner elements into the outer tsr element
tsr.append(s)

response = d.send_update_request(url=url,request=tsr)

