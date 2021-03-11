from requests.exceptions import HTTPError

from .rest_api_base import *

class SubscriptionMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def query_subscriptions(self, username_or_luid: Optional[str] = None, schedule_name_or_luid: Optional[str] = None,
                            subscription_subject: Optional[str] = None, view_or_workbook: Optional[str] = None,
                            content_name_or_luid: Optional[str] = None,
                            project_name_or_luid: Optional[str] = None,
                            wb_name_or_luid: Optional[str] = None) -> ET.Element:

        self.start_log_block()
        subscriptions = self.query_resource('subscriptions')
        filters_dict = {}
        if subscription_subject is not None:
            filters_dict['subject'] = '[@subject="{}"]'.format(subscription_subject)
        if schedule_name_or_luid is not None:
            if self.is_luid(schedule_name_or_luid):
                filters_dict['sched'] = 'schedule[@id="{}"'.format(schedule_name_or_luid)
            else:
                filters_dict['sched'] = 'schedule[@user="{}"'.format(schedule_name_or_luid)
        if username_or_luid is not None:
            if self.is_luid(username_or_luid):
                filters_dict['user'] = 'user[@id="{}"]'.format(username_or_luid)
            else:
                filters_dict['user'] = 'user[@name="{}"]'.format(username_or_luid)
        if view_or_workbook is not None:
            if view_or_workbook not in ['View', 'Workbook']:
                raise InvalidOptionException("view_or_workbook must be 'Workbook' or 'View'")
            # Does this search make sense my itself?

        if content_name_or_luid is not None:
            if self.is_luid(content_name_or_luid):
                filters_dict['content_luid'] = 'content[@id="{}"'.format(content_name_or_luid)
            else:
                if view_or_workbook is None:
                    raise InvalidOptionException('view_or_workbook must be specified for content: "Workbook" or "View"')
                if view_or_workbook == 'View':
                    if wb_name_or_luid is None:
                        raise InvalidOptionException('Must include wb_name_or_luid for a View name lookup')
                    content_luid = self.query_workbook_view_luid(wb_name_or_luid, content_name_or_luid,
                                                                 proj_name_or_luid=project_name_or_luid)
                elif view_or_workbook == 'Workbook':
                    content_luid = self.query_workbook_luid(content_name_or_luid, project_name_or_luid)
                filters_dict['content_luid'] = 'content[@id="{}"'.format(content_luid)

        if 'subject' in filters_dict:
            subscriptions = subscriptions.findall('.//t:subscription{}'.format(filters_dict['subject']), self.ns_map)
        if 'user' in filters_dict:
            subscriptions = subscriptions.findall('.//t:subscription/{}/..'.format(filters_dict['user']), self.ns_map)
        if 'sched' in filters_dict:
            subscriptions = subscriptions.findall('.//t:subscription/{}/..'.format(filters_dict['sched']), self.ns_map)
        if 'content_luid' in filters_dict:
            subscriptions = subscriptions.findall('.//t:subscription/{}/..'.format(filters_dict['content_luid']), self.ns_map)
        self.end_log_block()
        return subscriptions

    def create_subscription(self, subscription_subject: Optional[str] = None, view_or_workbook: Optional[str] = None,
                            content_name_or_luid: Optional[str] = None, schedule_name_or_luid: Optional[str] = None,
                            username_or_luid: Optional[str] = None, project_name_or_luid: Optional[str] = None,
                            wb_name_or_luid: Optional[str] = None,
                            direct_xml_request: Optional[ET.Element] = None) -> str:
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            if view_or_workbook not in ['View', 'Workbook']:
                raise InvalidOptionException("view_or_workbook must be 'Workbook' or 'View'")

            user_luid = self.query_user_luid(username_or_luid)
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

            if self.is_luid(content_name_or_luid):
                content_luid = content_name_or_luid
            else:
                if view_or_workbook == 'View':
                    if wb_name_or_luid is None:
                        raise InvalidOptionException('Must include wb_name_or_luid for a View name lookup')
                    content_luid = self.query_workbook_view_luid(wb_name_or_luid, content_name_or_luid,
                                                                 proj_name_or_luid=project_name_or_luid, username_or_luid=user_luid)
                elif view_or_workbook == 'Workbook':
                    content_luid = self.query_workbook_luid(content_name_or_luid, project_name_or_luid, user_luid)
                else:
                    raise InvalidOptionException("view_or_workbook must be 'Workbook' or 'View'")

            tsr = ET.Element('tsRequest')
            s = ET.Element('subscription')
            s.set('subject', subscription_subject)
            c = ET.Element('content')
            c.set('type', view_or_workbook)
            c.set('id', content_luid)
            sch = ET.Element('schedule')
            sch.set('id', schedule_luid)
            u = ET.Element('user')
            u.set('id', user_luid)
            s.append(c)
            s.append(sch)
            s.append(u)
            tsr.append(s)

        url = self.build_api_url('subscriptions')
        try:
            new_subscription = self.send_add_request(url, tsr)
            new_subscription_luid = new_subscription.findall('.//t:subscription', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_subscription_luid
        except RecoverableHTTPException as e:
            self.end_log_block()
            raise e
        except HTTPError as e:
            self.end_log_block()
            raise InvalidOptionException('Please check to make sure that you have an SMTP server configured and Subscriptions are enabled for this Server and Site')


    def create_subscription_to_workbook(self, subscription_subject: str, wb_name_or_luid: str, 
                                        schedule_name_or_luid: str, username_or_luid: str, 
                                        project_name_or_luid: Optional[str] = None) -> str:
        self.start_log_block()
        luid = self.create_subscription(subscription_subject=subscription_subject, view_or_workbook='Workbook',
                                        content_name_or_luid=wb_name_or_luid, schedule_name_or_luid=schedule_name_or_luid,
                                        username_or_luid=username_or_luid, project_name_or_luid=project_name_or_luid)
        self.end_log_block()
        return luid

    def create_subscription_to_view(self, subscription_subject: str, view_name_or_luid: str, schedule_name_or_luid: str,
                                    username_or_luid: str, wb_name_or_luid: Optional[str] = None, 
                                    project_name_or_luid: Optional[str] = None) -> str:
        self.start_log_block()
        luid = self.create_subscription(subscription_subject=subscription_subject, view_or_workbook='View',
                                        content_name_or_luid=view_name_or_luid, schedule_name_or_luid=schedule_name_or_luid,
                                        username_or_luid=username_or_luid, wb_name_or_luid=wb_name_or_luid,
                                        project_name_or_luid=project_name_or_luid)
        self.end_log_block()
        return luid

    def update_subscription(self, subscription_luid: str, subject: Optional[str] = None,
                            schedule_luid: Optional[str] = None) -> ET.Element:
        if subject is None and schedule_luid is None:
            raise InvalidOptionException("You must pass one of subject or schedule_luid, or both")
        tsr = ET.Element('tsRequest')
        s = ET.Element('subscription')

        if subject is not None:
            s.set('subject', subject)

        if schedule_luid is not None:
            sch = ET.Element('schedule')
            sch.set('id', schedule_luid)
            s.append(sch)
        tsr.append(s)

        url = self.build_api_url("subscriptions/{}".format(subscription_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def delete_subscriptions(self, subscription_luid_s: Union[List[str], str]):
        self.start_log_block()
        subscription_luids = self.to_list(subscription_luid_s)
        for subscription_luid in subscription_luids:
            url = self.build_api_url("subscriptions/{}".format(subscription_luid))
            self.send_delete_request(url)
        self.end_log_block()

class SubscriptionMethods27(SubscriptionMethods):
    def __init__(self, rest_api_base: TableauRestApiBase27):
        self.rest_api_base = rest_api_base
        
class SubscriptionMethods28(SubscriptionMethods27):
    def __init__(self, rest_api_base: TableauRestApiBase28):
        self.rest_api_base = rest_api_base

class SubscriptionMethods30(SubscriptionMethods28):
    def __init__(self, rest_api_base: TableauRestApiBase30):
        self.rest_api_base = rest_api_base

class SubscriptionMethods31(SubscriptionMethods30):
    def __init__(self, rest_api_base: TableauRestApiBase31):
        self.rest_api_base = rest_api_base

class SubscriptionMethods32(SubscriptionMethods31):
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base

class SubscriptionMethods33(SubscriptionMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

class SubscriptionMethods34(SubscriptionMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class SubscriptionMethods35(SubscriptionMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

    def create_subscription(self, subscription_subject: Optional[str] = None, view_or_workbook: Optional[str] = None,
                            content_name_or_luid: Optional[str] = None, schedule_name_or_luid: Optional[str] = None,
                            username_or_luid: Optional[str] = None, project_name_or_luid: Optional[str] = None,
                            wb_name_or_luid: Optional[str] = None,
                            image_attachment: bool = True, pdf_attachment: bool = False,
                            direct_xml_request: Optional[ET.Element] = None) -> str:
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            if view_or_workbook not in ['View', 'Workbook']:
                raise InvalidOptionException("view_or_workbook must be 'Workbook' or 'View'")

            user_luid = self.query_user_luid(username_or_luid)
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

            if self.is_luid(content_name_or_luid):
                content_luid = content_name_or_luid
            else:
                if view_or_workbook == 'View':
                    if wb_name_or_luid is None:
                        raise InvalidOptionException('Must include wb_name_or_luid for a View name lookup')
                    content_luid = self.query_workbook_view_luid(wb_name_or_luid, content_name_or_luid,
                                                                 proj_name_or_luid=project_name_or_luid, username_or_luid=user_luid)
                elif view_or_workbook == 'Workbook':
                    content_luid = self.query_workbook_luid(content_name_or_luid, project_name_or_luid, user_luid)
                else:
                    raise InvalidOptionException("view_or_workbook must be 'Workbook' or 'View'")

            tsr = ET.Element('tsRequest')
            s = ET.Element('subscription')
            s.set('subject', subscription_subject)
            s.set('attachImage', str(image_attachment).lower())
            s.set('attachPdf', str(pdf_attachment).lower())

            c = ET.Element('content')
            c.set('type', view_or_workbook)
            c.set('id', content_luid)
            sch = ET.Element('schedule')
            sch.set('id', schedule_luid)
            u = ET.Element('user')
            u.set('id', user_luid)
            s.append(c)
            s.append(sch)
            s.append(u)
            tsr.append(s)

        url = self.build_api_url('subscriptions')
        try:
            new_subscription = self.send_add_request(url, tsr)
            new_subscription_luid = new_subscription.findall('.//t:subscription', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_subscription_luid
        except RecoverableHTTPException as e:
            self.end_log_block()
            raise e
        except HTTPError as e:
            self.end_log_block()
            raise InvalidOptionException('Please check to make sure that you have an SMTP server configured and Subscriptions are enabled for this Server and Site')


    def update_subscription(self, subscription_luid: str, subject: Optional[str] = None,
                            schedule_luid: Optional[str] = None, image_attachment: Optional[bool] = None,
                            pdf_attachment: Optional[bool] = None) -> ET.Element:
        if subject is None and schedule_luid is None:
            raise InvalidOptionException("You must pass one of subject or schedule_luid, or both")
        tsr = ET.Element('tsRequest')
        s = ET.Element('subscription')

        if subject is not None:
            s.set('subject', subject)
        if image_attachment is not None:
            s.set('attachImage', str(image_attachment).lower())
        if pdf_attachment is not None:
            s.set('attachPdf', str(pdf_attachment).lower())

        if schedule_luid is not None:
            sch = ET.Element('schedule')
            sch.set('id', schedule_luid)
            s.append(sch)
        tsr.append(s)

        url = self.build_api_url("subscriptions/{}".format(subscription_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

class SubscriptionMethods36(SubscriptionMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base