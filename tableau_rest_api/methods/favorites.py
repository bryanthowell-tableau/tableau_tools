from .rest_api_base import *

import xml.etree.ElementTree as ET

class FavoritesMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def add_workbook_to_user_favorites(self, favorite_name: str, wb_name_or_luid: str,
                                       username_or_luid: str, proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        user_luid =  self.query_user_luid(username_or_luid)

        tsr = ET.Element('tsRequest')
        f = ET.Element('favorite')
        f.set('label', favorite_name)
        w = ET.Element('workbook')
        w.set('id', wb_luid)
        f.append(w)
        tsr.append(f)

        url = self.build_api_url("favorites/{}".format(user_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    def add_view_to_user_favorites(self, favorite_name: str, username_or_luid: str,
                                   view_name_or_luid: Optional[str]= None, view_content_url: Optional[str] = None,
                                   wb_name_or_luid: Optional[str] = None,
                                   proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()

        view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name_or_luid, view_content_url,
                                                  proj_name_or_luid)
        self.log('View luid found {}'.format(view_luid))

        user_luid = self.query_user_luid(username_or_luid)
        tsr = ET.Element('tsRequest')
        f = ET.Element('favorite')
        f.set('label', favorite_name)
        v = ET.Element('view')
        v.set('id', view_luid)
        f.append(v)
        tsr.append(f)

        url = self.build_api_url("favorites/{}".format(user_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    def query_user_favorites(self, username_or_luid: str) -> ET.Element:
        self.start_log_block()
        user_luid = self.query_user_luid(username_or_luid)
        favorites = self.query_resource("favorites/{}/".format(user_luid))
        self.end_log_block()
        return favorites

    def query_user_favorites_json(self, username_or_luid: str, page_number: Optional[int] = None) -> str:
        self.start_log_block()
        user_luid = self.query_user_luid(username_or_luid)
        favorites = self.query_resource_json("favorites/{}/".format(user_luid), page_number=page_number)
        self.end_log_block()
        return favorites

    # Can take collection or luid_string
    def delete_workbooks_from_user_favorites(self, wb_name_or_luid_s: Union[List[str], str],
                                             username_or_luid: str):
        self.start_log_block()
        wbs = self.to_list(wb_name_or_luid_s)
        user_luid = self.query_user_luid(username_or_luid)
        for wb in wbs:
            wb_luid = self.query_workbook_luid(wb)
            url = self.build_api_url("favorites/{}/workbooks/{}".format(user_luid, wb_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def delete_views_from_user_favorites(self, view_name_or_luid_s: Union[List[str], str],
                                         username_or_luid: str, wb_name_or_luid: Optional[str] = None):
        self.start_log_block()
        views = self.to_list(view_name_or_luid_s)

        user_luid = self.query_user_luid(username_or_luid)
        for view in views:

            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view)
            url = self.build_api_url("favorites/{}/views/{}".format(user_luid, view_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def add_datasource_to_user_favorites(self, favorite_name: str, ds_name_or_luid: str,
                                         username_or_luid: str, p_name_or_luid: Optional[str] = None):
        self.start_log_block()
        user_luid = self.query_user_luid(username_or_luid)

        datasource_luid = self.query_datasource_luid(ds_name_or_luid, p_name_or_luid)

        tsr = ET.Element('tsRequest')
        f = ET.Element('favorite')
        f.set('label', favorite_name)
        d = ET.Element('datasource')
        d.set('id', datasource_luid)
        f.append(d)
        tsr.append(f)

        url = self.build_api_url("favorites/{}".format(user_luid))
        self.send_update_request(url, tsr)

        self.end_log_block()

    def delete_datasources_from_user_favorites(self, ds_name_or_luid_s: Union[List[str], str],
                                               username_or_luid: str, p_name_or_luid: Optional[str] = None):
        self.start_log_block()
        dses = self.to_list(ds_name_or_luid_s)
        user_luid = self.query_user_luid(username_or_luid)
        for ds in dses:
            ds_luid = self.query_datasource_luid(ds, p_name_or_luid)
            url = self.build_api_url("favorites/{}/datasources/{}".format(user_luid, ds_luid))
            self.send_delete_request(url)
        self.end_log_block()

class FavoritesMethods27(FavoritesMethods):
    def __init__(self, rest_api_base: TableauRestApiBase27):
        self.rest_api_base = rest_api_base

class FavoritesMethods28(FavoritesMethods27):
    def __init__(self, rest_api_base: TableauRestApiBase28):
        self.rest_api_base = rest_api_base

class FavoritesMethods30(FavoritesMethods28):
    def __init__(self, rest_api_base: TableauRestApiBase30):
        self.rest_api_base = rest_api_base

class FavoritesMethods31(FavoritesMethods30):
    def __init__(self, rest_api_base: TableauRestApiBase31):
        self.rest_api_base = rest_api_base

    def add_project_to_user_favorites(self, favorite_name: str, proj_name_or_luid: str,
                                      username_or_luid: str) -> ET.Element:
        self.start_log_block()
        proj_luid = self.query_project_luid(proj_name_or_luid)
        user_luid = self.query_user_luid(username_or_luid)
        tsr = ET.Element('tsRequest')
        f = ET.Element('favorite')
        f.set('label', favorite_name)
        w = ET.Element('project')
        w.set('id', proj_luid)
        f.append(w)
        tsr.append(f)

        url = self.build_api_url("favorites/{}".format(user_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    def delete_projects_from_user_favorites(self, proj_name_or_luid_s: Union[List[str], str],
                                            username_or_luid:str):
        self.start_log_block()
        projs = self.to_list(proj_name_or_luid_s)
        user_luid = self.query_user_luid(username_or_luid)
        for proj in projs:
            proj_luid = self.query_project_luid(proj)
            url = self.build_api_url("favorites/{}/projects/{}".format(user_luid, proj_luid))
            self.send_delete_request(url)
        self.end_log_block()

class FavoritesMethods32(FavoritesMethods31):
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base

class FavoritesMethods33(FavoritesMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

class FavoritesMethods34(FavoritesMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class FavoritesMethods35(FavoritesMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

class FavoritesMethods36(FavoritesMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base