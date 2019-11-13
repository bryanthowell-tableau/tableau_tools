from .rest_api_base import *

# First Alert Methods appear in API 3.2
class AlertMethods32():
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def query_data_driven_alerts(self) -> ET.Element:
        self.start_log_block()
        alerts = self.query_resource("dataAlerts")
        self.end_log_block()
        return alerts

    def query_data_driven_alerts_for_view(self, view_luid: str) -> ET.Element:
        self.start_log_block()
        alerts = self.query_resource("dataAlerts?filter=viewId:eq:{}".format(view_luid))
        self.end_log_block()
        return alerts

    def query_data_driven_alert_details(self, data_alert_luid: str) -> ET.Element:
        self.start_log_block()
        alert_details = self.query_resource("dataAlerts/{}".format(data_alert_luid))
        self.end_log_block()
        return alert_details

    def delete_data_driven_alert(self, data_alert_luid: str):
        self.start_log_block()
        url = self.build_api_url("dataAlerts/{}".format(data_alert_luid))
        self.send_delete_request(url)
        self.end_log_block()

    def add_user_to_data_driven_alert(self, data_alert_luid: str, username_or_luid: str):
        self.start_log_block()
        user_luid = self.query_user_luid(username_or_luid)

        tsr = ET.Element("tsRequest")
        u = ET.Element("user")
        u.set("id", user_luid)
        tsr.append(u)
        url = self.build_api_url('dataAlerts/{}/users'.format(data_alert_luid))
        self.send_add_request(url, tsr)
        self.end_log_block()

    def update_data_driven_alert(self, data_alert_luid: str, subject: Optional[str] = None,
                                 frequency: Optional[str] = None,
                                 owner_username_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        tsr = ET.Element("tsRequest")
        d = ET.Element("dataAlert")
        if subject is not None:
            d.set("subject", subject)

        if frequency is not None:
            frequency = frequency.lower()
            allowed_frequency = ('once', 'frequently', 'hourly', 'daily', 'weekly')
            if frequency not in allowed_frequency:
                raise InvalidOptionException('frequency must be once, frequently, hourly, daily or weekly')
            d.set('frequency', frequency)

        if owner_username_or_luid is not None:
            owner_luid = self.query_user_luid(owner_username_or_luid)
            o = ET.Element('owner')
            o.set("id", owner_luid)
            d.append(o)

        tsr.append(d)
        url = self.build_api_url("dataAlerts/{}".format(data_alert_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def delete_user_from_data_driven_alert(self, data_alert_luid: str, username_or_luid: str):
        self.start_log_block()
        user_luid = self.query_user_luid(username_or_luid)

        url = self.build_api_url('dataAlerts/{}/users/{}'.format(data_alert_luid, user_luid))
        self.send_delete_request(url)
        self.end_log_block()

class AlertMethods33(AlertMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

class AlertMethods34(AlertMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class AlertMethods35(AlertMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

class AlertMethods36(AlertMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base