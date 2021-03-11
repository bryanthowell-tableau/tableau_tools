from .rest_api_base import *

# First Metadata Methods appear in API 3.5
class WebhooksMethods36():
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def create_webhook(self, webhook_name: str, webhook_source_api_event_name: str, url: str) -> ET.Element:
        self.start_log_block()
        tsr = ET.Element('tsRequest')
        wh = ET.Element('webhook')
        wh.set('name', webhook_name)

        whs = ET.Element('webhook-source')
        whs_event = ET.Element(webhook_source_api_event_name)
        whs.append(whs_event)

        whd = ET.Element('webhook-destination')

        whdh = ET.Element('webhook-destination-http')
        whdh.set('method', 'POST')
        whdh.set('url', url)
        whd.append(whd)

        wh.append(whs)
        wh.append(whd)

        tsr.append(wh)
        url = self.build_api_url("webhooks")
        response = self.send_add_request(url=url, request=tsr)
        self.end_log_block()
        return response

    def list_webhooks(self) -> ET.Element:
        self.start_log_block()
        response = self.query_resource("webhooks")
        self.end_log_block()
        return response

    def test_webhook(self, webhook_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        webhook_luid = self.query_webhook_luid(webhook_name_or_luid)
        response = self.query_resource("webhooks/{}/test".format(webhook_luid))
        self.end_log_block()
        return response

    def delete_webhook(self, webhook_name_or_luid: str):
        self.start_log_block()
        webhook_luid = self.query_webhook_luid(webhook_name_or_luid)
        url = self.build_api_url("webhooks/{}".format(webhook_luid))
        self.delete_resource(url)
        self.end_log_block()

    def get_webhook(self, webhook_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        webhook_luid = self.query_webhook_luid(webhook_name_or_luid)
        response = self.query_resource("webhooks/{}".format(webhook_luid))
        self.end_log_block()
        return response
