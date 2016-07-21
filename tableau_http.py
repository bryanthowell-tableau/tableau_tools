import urllib2
from tableau_exceptions import *


# Class for direct requests to the Tableau Sever only over HTTP
class TableauHTTP:
    def __init__(self, tableau_server_url):
        self.tableau_server_url = tableau_server_url

    def get_trusted_ticket_for_user(self, username, site='default', ip=None):
        trusted_url = self.tableau_server_url + "/trusted"
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(trusted_url)
        post_data = u"username={}".format(username)
        if site.lower() != 'default':
            post_data += u"&target_site={}".format(site)
        request.add_data(post_data)
        trusted_ticket_response = opener.open(request)
        try:
            ticket = trusted_ticket_response.read()
            if ticket == '-1' or not ticket:
                raise NoResultsException('Ticket generation was not complete.')
            else:
                return ticket
        except urllib2.HTTPError as e:
            if e.code >= 500:
                raise
            raw_error_response = e.fp.read()
            # self.log(u"Received a {} error, here was response:".format(unicode(e.code)))

    # When establishing a trusted ticket session, all you want is a good response. You don't actually do anything with
    # the ticket or want to load the viz.
    def redeem_trusted_ticket(self, view_to_redeem, trusted_ticket, site='default'):
        trusted_view_url = "{}/trusted/{}".format(self.tableau_server_url, trusted_ticket)
        if site.lower() != 'default':
            trusted_view_url += "/t/{}/views/{}".format(site, view_to_redeem)
        else:
            trusted_view_url += "/views/{}".format(view_to_redeem)

        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(trusted_view_url)
        try:
            response = opener.open(request)
        except urllib2.HTTPError as e:
            if e.code >= 500:
                raise
            raw_error_response = e.fp.read()
            # self.log(u"Received a {} error, here was response:".format(unicode(e.code)))

    def create_trusted_ticket_session(self, view_to_redeem, username, site='default', ip=None):
        ticket = self.get_trusted_ticket_for_user(username, site=site, ip=ip)
        self.redeem_trusted_ticket(view_to_redeem, ticket, site=site)
