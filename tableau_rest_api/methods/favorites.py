from .rest_api_base import *

import xml.etree.ElementTree as ET

class FavoritesMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest = rest_api_base

    #def __getattr__(self, attr):
    #    return getattr(self.rest_api_base, attr)

    def add_workbook_to_user_favorites(self, favorite_name: str, wb_name_or_luid: str,
                                       username_or_luid: str, proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        user_luid =  self.rest.query_user_luid(username_or_luid)

        tsr = ET.Element('tsRequest')
        f = ET.Element('favorite')
        f.set('label', favorite_name)
        w = ET.Element('workbook')
        w.set('id', wb_luid)
        f.append(w)
        tsr.append(f)

        url = self.rest.build_api_url("favorites/{}".format(user_luid))
        update_response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return update_response

    def add_view_to_user_favorites(self, favorite_name: str, username_or_luid: str,
                                   view_name_or_luid: Optional[str]= None, view_content_url: Optional[str] = None,
                                   wb_name_or_luid: Optional[str] = None,
                                   proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()

        view_luid = self.rest.query_workbook_view_luid(wb_name_or_luid, view_name_or_luid, view_content_url,
                                                  proj_name_or_luid)
        self.rest.log('View luid found {}'.format(view_luid))

        user_luid = self.rest.query_user_luid(username_or_luid)
        tsr = ET.Element('tsRequest')
        f = ET.Element('favorite')
        f.set('label', favorite_name)
        v = ET.Element('view')
        v.set('id', view_luid)
        f.append(v)
        tsr.append(f)

        url = self.rest.build_api_url("favorites/{}".format(user_luid))
        update_response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return update_response

    def query_user_favorites(self, username_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        user_luid = self.rest.query_user_luid(username_or_luid)
        favorites = self.rest.query_resource("favorites/{}/".format(user_luid))
        self.rest.end_log_block()
        return favorites

    def query_user_favorites_json(self, username_or_luid: str, page_number: Optional[int] = None) -> str:
        self.rest.start_log_block()
        user_luid = self.rest.query_user_luid(username_or_luid)
        favorites = self.rest.query_resource_json("favorites/{}/".format(user_luid), page_number=page_number)
        self.rest.end_log_block()
        return favorites

    # Can take collection or luid_string
    def delete_workbooks_from_user_favorites(self, wb_name_or_luid_s: Union[List[str], str],
                                             username_or_luid: str):
        self.rest.start_log_block()
        wbs = self.rest.to_list(wb_name_or_luid_s)
        user_luid = self.rest.query_user_luid(username_or_luid)
        for wb in wbs:
            wb_luid = self.rest.query_workbook_luid(wb)
            url = self.rest.build_api_url("favorites/{}/workbooks/{}".format(user_luid, wb_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()

    def delete_views_from_user_favorites(self, view_name_or_luid_s: Union[List[str], str],
                                         username_or_luid: str, wb_name_or_luid: Optional[str] = None):
        self.rest.start_log_block()
        views = self.rest.to_list(view_name_or_luid_s)

        user_luid = self.rest.query_user_luid(username_or_luid)
        for view in views:

            view_luid = self.rest.query_workbook_view_luid(wb_name_or_luid, view)
            url = self.rest.build_api_url("favorites/{}/views/{}".format(user_luid, view_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()

    def add_datasource_to_user_favorites(self, favorite_name: str, ds_name_or_luid: str,
                                         username_or_luid: str, p_name_or_luid: Optional[str] = None):
        self.rest.start_log_block()
        user_luid = self.rest.query_user_luid(username_or_luid)

        datasource_luid = self.rest.query_datasource_luid(ds_name_or_luid, p_name_or_luid)

        tsr = ET.Element('tsRequest')
        f = ET.Element('favorite')
        f.set('label', favorite_name)
        d = ET.Element('datasource')
        d.set('id', datasource_luid)
        f.append(d)
        tsr.append(f)

        url = self.rest.build_api_url("favorites/{}".format(user_luid))
        self.rest.send_update_request(url, tsr)

        self.rest.end_log_block()

    def delete_datasources_from_user_favorites(self, ds_name_or_luid_s: Union[List[str], str],
                                               username_or_luid: str, p_name_or_luid: Optional[str] = None):
        self.rest.start_log_block()
        dses = self.rest.to_list(ds_name_or_luid_s)
        user_luid = self.rest.query_user_luid(username_or_luid)
        for ds in dses:
            ds_luid = self.rest.query_datasource_luid(ds, p_name_or_luid)
            url = self.rest.build_api_url("favorites/{}/datasources/{}".format(user_luid, ds_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()

    def add_project_to_user_favorites(self, favorite_name: str, proj_name_or_luid: str,
                                      username_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        proj_luid = self.rest.query_project_luid(proj_name_or_luid)
        user_luid = self.rest.query_user_luid(username_or_luid)
        tsr = ET.Element('tsRequest')
        f = ET.Element('favorite')
        f.set('label', favorite_name)
        w = ET.Element('project')
        w.set('id', proj_luid)
        f.append(w)
        tsr.append(f)

        url = self.rest.build_api_url("favorites/{}".format(user_luid))
        update_response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return update_response

    def delete_projects_from_user_favorites(self, proj_name_or_luid_s: Union[List[str], str],
                                            username_or_luid:str):
        self.rest.start_log_block()
        projs = self.rest.to_list(proj_name_or_luid_s)
        user_luid = self.rest.query_user_luid(username_or_luid)
        for proj in projs:
            proj_luid = self.rest.query_project_luid(proj)
            url = self.rest.build_api_url("favorites/{}/projects/{}".format(user_luid, proj_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()


