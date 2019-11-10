from .rest_api_base import *

class FavoritesMethods(TableauRestApiBase):
    def add_workbook_to_user_favorites(self, favorite_name: str, wb_name_or_luid: str,
                                       username_or_luid: str, proj_name_or_luid: Optional[str] = None) -> etree.Element:
        self.start_log_block()
        wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)
        user_luid =  self.query_user_luid(username_or_luid)

        tsr = etree.Element('tsRequest')
        f = etree.Element('favorite')
        f.set('label', favorite_name)
        w = etree.Element('workbook')
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
                                   proj_name_or_luid: Optional[str] = None) -> etree.Element:
        self.start_log_block()
        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            if wb_name_or_luid is None:
                raise InvalidOptionException('When passing a View Name instead of LUID, must also specify workbook name or luid')
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name_or_luid, view_content_url,
                                                      proj_name_or_luid, username_or_luid)
            self.log('View luid found {}'.format(view_luid))

        user_luid = self.query_user_luid(username_or_luid)
        tsr = etree.Element('tsRequest')
        f = etree.Element('favorite')
        f.set('label', favorite_name)
        v = etree.Element('view')
        v.set('id', view_luid)
        f.append(v)
        tsr.append(f)

        url = self.build_api_url("favorites/{}".format(user_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    #
    # End Add methods
    #

    def query_user_favorites(self, username_or_luid):
        """
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        favorites = self.query_resource("favorites/{}/".format(user_luid))

        self.end_log_block()
        return favorites

    def query_user_favorites_json(self, username_or_luid, page_number=None):
        """
        :type username_or_luid: unicode
        :rtype: json
        """
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        favorites = self.query_resource_json("favorites/{}/".format(user_luid), page_number=page_number)

        self.end_log_block()
        return favorites

    # Can take collection or luid_string
    def delete_workbooks_from_user_favorites(self, wb_name_or_luid_s, username_or_luid):
        """
        :type wb_name_or_luid_s: List[unicode] or unicode
        :type username_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        wbs = self.to_list(wb_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        for wb in wbs:
            if self.is_luid(wb):
                wb_luid = wb
            else:
                wb_luid = self.query_workbook_luid(wb)
            url = self.build_api_url("favorites/{}/workbooks/{}".format(user_luid, wb_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def delete_views_from_user_favorites(self, view_name_or_luid_s, username_or_luid, wb_name_or_luid=None):
        """
        :type view_name_or_luid_s: List[unicode] or unicode
        :type username_or_luid: unicode
        :type wb_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        views = self.to_list(view_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        for view in views:
            if self.is_luid(view):
                view_luid = view
            else:
                view_luid = self.query_workbook_view_luid(wb_name_or_luid, view)
            url = self.build_api_url("favorites/{}/views/{}".format(user_luid, view_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def add_datasource_to_user_favorites(self, favorite_name, ds_name_or_luid_s, username_or_luid, p_name_or_luid=None):
        """
        :type favorite_name: unicode
        :type ds_name_or_luid_s: unicode
        :type username_or_luid: unicode
        :type p_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        dses = self.to_list(ds_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        for ds in dses:
            if self.is_luid(ds_name_or_luid_s):
                datasource_luid = ds
            else:
                datasource_luid = self.query_datasource_luid(ds, p_name_or_luid)

            tsr = etree.Element('tsRequest')
            f = etree.Element('favorite')
            f.set('label', favorite_name)
            d = etree.Element('datasource')
            d.set('id', datasource_luid)
            f.append(d)
            tsr.append(f)

            url = self.build_api_url("favorites/{}".format(user_luid))
            self.send_update_request(url, tsr)

        self.end_log_block()

    def delete_datasources_from_user_favorites(self, ds_name_or_luid_s, username_or_luid, p_name_or_luid=None):
        """
        :type ds_name_or_luid_s: List[unicode] or unicode
        :type username_or_luid: unicode
        :type p_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        dses = self.to_list(ds_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        for ds in dses:
            if self.is_luid(ds):
                ds_luid = ds
            else:
                ds_luid = self.query_datasource_luid(ds, p_name_or_luid)
            url = self.build_api_url("favorites/{}/datasources/{}".format(user_luid, ds_luid))
            self.send_delete_request(url)
        self.end_log_block()