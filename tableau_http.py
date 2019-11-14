import urllib.request, urllib.error, urllib.parse
import requests
from .tableau_exceptions import *
from typing import Union, Any, Optional, List, Dict, Tuple

# Class for direct requests to the Tableau Sever only over HTTP
class TableauHTTP:
    def __init__(self, tableau_server_url: str):
        self.tableau_server_url = tableau_server_url
        self.session = requests.Session()

    def get_trusted_ticket_for_user(self, username: str, site:str = 'default', client_ip: Optional[str] = None):
        trusted_url = self.tableau_server_url + "/trusted"
        self.session = requests.Session()
        post_data = "username={}".format(username)
        if site.lower() != 'default':
            post_data += "&target_site={}".format(site)
        if client_ip is not None:
            post_data += "&client_id={}".format(client_ip)
        response = self.session.post(trusted_url, data=post_data)
        ticket = response.content
        if ticket == '-1' or not ticket:
            raise NoResultsException('Ticket generation was not complete (returned "-1"), check Tableau Server trusted authorization configurations')
        else:
            return ticket

    # When establishing a trusted ticket session, all you want is a good response. You don't actually do anything with
    # the ticket or want to load the viz.
    # Fastest way is to add .png
    def redeem_trusted_ticket(self, view_to_redeem: str, trusted_ticket: str, site: str = 'default') -> requests.Response:
        trusted_view_url = "{}/trusted/{}".format(self.tableau_server_url, trusted_ticket)
        if site.lower() != 'default':
            trusted_view_url += "/t/{}/views/{}.png".format(site, view_to_redeem)
        else:
            trusted_view_url += "/views/{}.png".format(view_to_redeem)
        response = self.session.get(trusted_view_url)
        return response


    def create_trusted_ticket_session(self, view_to_redeem: str, username: str, site: str = 'default',
                                      client_ip: Optional[str] = None):
        ticket = self.get_trusted_ticket_for_user(username, site=site,  client_ip=client_ip)
        self.redeem_trusted_ticket(view_to_redeem, ticket, site=site)
