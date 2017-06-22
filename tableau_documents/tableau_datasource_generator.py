import datetime
from xml.sax.saxutils import quoteattr

from ..tableau_base import *
from ..tableau_exceptions import *

import zipfile
import os


class TableauDatasourceGenerator(TableauBase):
    def __init__(self, ds_type, ds_name, server, dbname, logger_obj, authentication=u'username-password',
                 initial_sql=None):
        super(self.__class__, self).__init__()
        self.logger = logger_obj
        self.log(u'Initializing a TableauDatasourceGenerator object')
        self.ds_class = None
        self.ds_name = ds_name
        if ds_type in self.datasource_class_map:
            self.ds_class = self.datasource_class_map[ds_type]
        elif ds_type in self.datasource_class_map.values():
            self.ds_class = ds_type
        else:
            raise InvalidOptionException('{} is not an acceptable type'.format(ds_type))
        self.log("DS Class is {}".format(self.ds_class))
        self.nsmap = {u"user": u'http://www.tableausoftware.com/xml/user'}
        self.ds_xml = etree.Element(u"datasource", nsmap=self.nsmap)
        self.ds_xml.set(u'formatted-name', self.ds_class + u'.1ch1jwefjwfw')
        self.ds_xml.set(u'inline', u'true')
        self.ds_xml.set(u'version', u'9.3')
        self.server = server
        self.dbname = dbname
        self.authentication = authentication
        self.main_table_relation = None
        self.main_table_name = None
        self.join_relations = []
        self.connection = etree.Element(u"connection")
        self.connection.set(u'class', self.ds_class)
        self.connection.set(u'dbname', self.dbname)
        self.connection.set(u'odbc-native-protocol', u'yes')
        self.connection.set(u'server', self.server)
        self.connection.set(u'authentication', u'sspi')
        if initial_sql is not None:
            self.connection.set(u'one-time-sql', initial_sql)
        self.tde_filename = None
        self.incremental_refresh_field = None
        self.column_mapping = {}
        self.column_aliases = {}
        self.datasource_filters = []
        self.extract_filters = []
        self.initial_sql = None
        self.column_instances = []

    def add_first_table(self, db_table_name, table_alias):
        self.main_table_relation = self.create_table_relation(db_table_name, table_alias)

    def add_first_custom_sql(self, custom_sql, table_alias):
        self.main_table_relation = self.create_custom_sql_relation(custom_sql, table_alias)

    @staticmethod
    def create_random_calculation_name():
        n = 19
        range_start = 10 ** (n - 1)
        range_end = (10 ** n) - 1
        random_digits = random.randint(range_start, range_end)
        return u'Calculation_{}'.format(unicode(random_digits))

    @staticmethod
    def create_table_relation(db_table_name, table_alias):
        r = etree.Element(u"relation")
        r.set(u'name', table_alias)
        r.set(u"table", u"[{}]".format(db_table_name))
        r.set(u"type", u"table")
        return r

    @staticmethod
    def create_custom_sql_relation(custom_sql, table_alias):
        r = etree.Element(u"relation")
        r.set(u'name', table_alias)
        r.text = custom_sql
        r.set(u"type", u"text")
        return r

    # on_clauses = [ { left_table_alias : , left_field : , operator : right_table_alias : , right_field : ,  },]
    @staticmethod
    def define_join_on_clause(left_table_alias, left_field, operator, right_table_alias, right_field):
        return {u"left_table_alias": left_table_alias,
                u"left_field": left_field,
                u"operator": operator,
                u"right_table_alias": right_table_alias,
                u"right_field": right_field
                }

    def join_table(self, join_type, db_table_name, table_alias, join_on_clauses, custom_sql=None):
        full_join_desc = {u"join_type": join_type.lower(),
                          u"db_table_name": db_table_name,
                          u"table_alias": table_alias,
                          u"on_clauses": join_on_clauses,
                          u"custom_sql": custom_sql}
        self.join_relations.append(full_join_desc)

    def generate_relation_section(self):
        # Because of the strange way that the interior definition is the last on, you need to work inside out
        # "Middle-out" as Silicon Valley suggests.
        # Generate the actual JOINs

        # There's only a single main relation with only one table
        if len(self.join_relations) == 0:
            self.connection.append(self.main_table_relation)
            self.ds_xml.append(self.connection)
        else:
            prev_relation = self.main_table_relation
            # We go through each relation, build the whole thing, then append it to the previous relation, then make
            # that the new prev_relationship. Something like recurssion
            for join_desc in self.join_relations:
                r = etree.Element(u"relation")
                r.set(u"join", join_desc[u"join_type"])
                r.set(u"type", u"join")
                if len(join_desc[u"on_clauses"]) == 0:
                    raise InvalidOptionException("Join clause must have at least one ON clause describing relation")
                else:
                    and_expression = None
                    if len(join_desc[u"on_clauses"]) > 1:
                        and_expression = etree.Element(u"expression")
                        and_expression.set(u"op", u'AND')
                    for on_clause in join_desc[u"on_clauses"]:
                        c = etree.Element(u"clause")
                        c.set(u"type", u"join")
                        e = etree.Element(u"expression")
                        e.set(u"op", on_clause[u"operator"])

                        e_field1 = etree.Element(u"expression")
                        e_field1_name = u'[{}].[{}]'.format(on_clause[u"left_table_alias"],
                                                            on_clause[u"left_field"])
                        e_field1.set(u"op", e_field1_name)
                        e.append(e_field1)

                        e_field2 = etree.Element(u"expression")
                        e_field2_name = u'[{}].[{}]'.format(on_clause[u"right_table_alias"],
                                                            on_clause[u"right_field"])
                        e_field2.set(u"op", e_field2_name)
                        e.append(e_field2)
                        if and_expression is not None:
                            and_expression.append(e)
                        else:
                            and_expression = e

                c.append(and_expression)
                r.append(c)
                r.append(prev_relation)

                if join_desc[u"custom_sql"] is None:
                    new_table_rel = self.create_table_relation(join_desc[u"db_table_name"],
                                                               join_desc[u"table_alias"])
                elif join_desc[u"custom_sql"] is not None:
                    new_table_rel = self.create_custom_sql_relation(join_desc[u'custom_sql'],
                                                                    join_desc[u'table_alias'])
                r.append(new_table_rel)
                prev_relation = r

                self.connection.append(prev_relation)
                self.ds_xml.append(self.connection)

    def add_table_column(self, table_alias, table_field_name, tableau_field_alias):
        # Check to make sure the alias has been added

        # Check to make sure the tableau_field_alias hasn't been used already

        self.column_mapping[tableau_field_alias] = u"[{}].[{}]".format(table_alias, table_field_name)

    def add_column_alias(self, tableau_field_alias, caption=None, dimension_or_measure=None,
                         discrete_or_continuous=None, datatype=None, calculation=None):
        if dimension_or_measure.lower() in [u'dimension', u'measure']:
            role = dimension_or_measure.lower()
        else:
            raise InvalidOptionException("{} should be either measure or dimension".format(dimension_or_measure))

        if discrete_or_continuous.lower() in [u'discrete', u'continuous']:
            if discrete_or_continuous.lower() == u'discrete':
                if datatype.lower() in [u'string']:
                    t_type = u'nominal'
                else:
                    t_type = u'ordinal'
            elif discrete_or_continuous.lower() == u'continuous':
                t_type = u'quantitative'
        else:
            raise InvalidOptionException("{} should be either discrete or continuous".format(discrete_or_continuous))

        if datatype.lower() not in [u'string', u'integer', u'datetime', u'date', u'real', u'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))

        self.column_aliases[tableau_field_alias] = {u"caption": caption,
                                                    u"type": t_type,
                                                    u"datatype": datatype.lower(),
                                                    u"role": role,
                                                    u"calculation": calculation}

    def add_calculation(self, calculation, calculation_name, dimension_or_measure, discrete_or_continuous, datatype):
        internal_calc_name = self.create_random_calculation_name()
        self.add_column_alias(internal_calc_name, calculation_name, dimension_or_measure, discrete_or_continuous,
                              datatype, calculation)
        # internal_calc_name allows you to create a filter on this
        return internal_calc_name

    @staticmethod
    def create_dimension_filter(column_name, values, include_or_exclude=u'include', custom_value_list=False):
        # Check if column_name is actually the alias of a calc, if so, replace with the random internal calc name

        if include_or_exclude.lower() in [u'include', u'exclude']:
            if include_or_exclude.lower() == u'include':
                ui_enumeration = u'inclusive'
            elif include_or_exclude.lower() == u'exclude':
                ui_enumeration = u'exclusive'
        else:
            raise InvalidOptionException('{} is not valid, must be include or exclude'.format(include_or_exclude))
        ds_filter = {
                     u"type": u'categorical',
                     u'column_name': u'[{}]'.format(column_name),
                     u"values": values,
                     u'ui-enumeration': ui_enumeration,
                     u'ui-manual-selection': custom_value_list
                    }
        return ds_filter

    def add_dimension_datasource_filter(self, column_name, values, include_or_exclude=u'include',
                                        custom_value_list=False):
        ds_filter = self.create_dimension_filter(column_name, values, include_or_exclude, custom_value_list)
        self.datasource_filters.append(ds_filter)

    def add_dimension_extract_filter(self, column_name, values, include_or_exclude=u'include', custom_value_list=False):
        ds_filter = self.create_dimension_filter(column_name, values, include_or_exclude, custom_value_list)
        self.extract_filters.append(ds_filter)

    def add_continuous_datasource_filter(self, column_name, min_value=None, max_value=None, date=False):
        ds_filter = self.create_continuous_filter(column_name, min_value=min_value, max_value=max_value, date=date)
        self.datasource_filters.append(ds_filter)

    def add_continuous_extract_filter(self, column_name, min_value=None, max_value=None, date=False):
        ds_filter = self.create_continuous_filter(column_name, min_value=min_value, max_value=max_value ,date=date)
        self.extract_filters.append(ds_filter)

    def add_relative_date_datasource_filter(self, column_name, period_type, number_of_periods=None,
                                            previous_next_current=u'previous', to_date=False):
        ds_filter = self.create_relative_date_filter(column_name, period_type, number_of_periods, previous_next_current
                                                     , to_date)
        self.datasource_filters.append(ds_filter)

    def add_relative_date_extract_filter(self, column_name, period_type, number_of_periods=None,
                                         previous_next_current=u'previous', to_date=False):
        ds_filter = self.create_relative_date_filter(column_name, period_type, number_of_periods, previous_next_current,
                                                     to_date)
        self.extract_filters.append(ds_filter)

    def create_continuous_filter(self, column_name, min_value=None, max_value=None, date=False):
        # Dates need to be wrapped in # #
        if date is True:
            if min_value is not None:
                min_value = u'#{}#'.format(unicode(min_value))
            if max_value is not None:
                max_value = u'#{}#'.format(unicode(max_value))
            final_column_name = u'[none:{}:qk]'.format(column_name)
            # Need to create a column-instance tag in the columns section
            self.column_instances.append(
                {
                    u'column': u'[{}]'.format(column_name),
                    u'name': final_column_name,
                    u'type': u'quantitative'
                }
            )
        else:
            final_column_name = u'[{}]'.format(column_name)
        ds_filter = {
            u'type': u'quantitative',
            u'min': min_value,
            u'max': max_value,
            u'column_name': final_column_name,
        }
        return ds_filter

    def create_relative_date_filter(self, column_name, period_type, number_of_periods,
                                    previous_next_current=u'previous', to_date=False):
        if period_type.lower() not in [u'quarter', u'year', u'month', u'day', u'hour', u'minute']:
            raise InvalidOptionException('period_type must be one of : quarter, year, month, day, hour, minute')
        # Need to create a column-instance tag in the columns section
        final_column_name = u'[none:{}:qk]'.format(column_name)
        # Need to create a column-instance tag in the columns section
        self.column_instances.append(
            {
                u'column': u'[{}]'.format(column_name),
                u'name': final_column_name,
                u'type': u'quantitative'
            }
        )
        if previous_next_current.lower() == u'previous':
            first_period = u'-{}'.format(unicode(number_of_periods))
            last_period = u'0'
        elif previous_next_current.lower() == u'next':
            first_period = u'1'
            last_period = u'{}'.format(unicode(number_of_periods))
        elif previous_next_current.lower() == u'current':
            first_period = u'0'
            last_period = u'0'
        else:
            raise InvalidOptionException('You must use "previous", "next" or "current" for the period selections')

        if to_date is False:
            include_future = u'true'
        elif to_date is True:
            include_future = u'false'

        ds_filter = {
            u'type': u'relative-date',
            u'column_name': final_column_name,
            u'first-period': first_period,
            u'last-period': last_period,
            u'period-type': period_type.lower(),
            u'include-future': include_future
        }
        return ds_filter

    def generate_filters(self, filter_array):
        return_array = []
        for filter_def in filter_array:
            f = etree.Element(u'filter')
            f.set(u'class', filter_def[u'type'])
            f.set(u'column', filter_def[u'column_name'])
            f.set(u'filter-group', u'2')
            if filter_def[u'type'] == u'quantitative':
                f.set(u'include-values', u'in-range')
                if filter_def[u'min'] is not None:
                    m = etree.Element(u'min')
                    m.text = unicode(filter_def[u'min'])
                    f.append(m)
                if filter_def[u'max'] is not None:
                    m = etree.Element(u'max')
                    m.text = unicode(filter_def[u'max'])
                    f.append(m)
            elif filter_def[u'type'] == u'relative-date':
                f.set(u'first-period', filter_def[u'first-period'])
                f.set(u'include-future', filter_def[u'include-future'])
                f.set(u'last-period', filter_def[u'last-period'])
                f.set(u'include-null', u'false')
                f.set(u'period-type', filter_def[u'period-type'])

            elif filter_def[u'type'] == u'categorical':
                gf = etree.Element(u'groupfilter')
                # This attribute has a user namespace
                gf.set(u'{' + u'{}'.format(self.nsmap[u'user']) + u'}ui-domain', u'database')
                gf.set(u'{' + u'{}'.format(self.nsmap[u'user']) + u'}ui-enumeration', filter_def[u'ui-enumeration'])
                gf.set(u'{' + u'{}'.format(self.nsmap[u'user']) + u'}ui-marker', u'enumerate')
                if filter_def[u'ui-manual-selection'] is True:
                    gf.set(u'{' + u'{}'.format(self.nsmap[u'user']) + u'}ui-manual-selection', u'true')
                if len(filter_def[u'values']) == 1:
                    if filter_def[u'ui-enumeration'] == u'exclusive':
                        gf.set(u'function', u'except')
                        gf1 = etree.Element(u'groupfilter')
                        gf1.set(u'function', u'member')
                        gf1.set(u'level', filter_def[u'column_name'])
                    else:
                        gf.set(u'function', u'member')
                        gf.set(u'level', filter_def[u'column_name'])
                        gf1 = gf
                    # strings need the &quot;, ints do not
                    if isinstance(filter_def[u'values'][0], basestring):
                        gf1.set(u'member', quoteattr(filter_def[u'values'][0]))
                    else:
                        gf1.set(u'member', unicode(filter_def[u'values'][0]))
                    if filter_def[u'ui-enumeration'] == u'exclusive':
                        # Single exclude filters include an extra groupfilter set with level-members function
                        lm = etree.Element(u'groupfilter')
                        lm.set(u'function', u'level-members')
                        lm.set(u'level', filter_def[u'column_name'])
                        gf.append(lm)
                        gf.append(gf1)
                    f.append(gf)
                else:
                    if filter_def[u'ui-enumeration'] == u'exclusive':
                        gf.set(u'function', u'except')
                    else:
                        gf.set(u'function', u'union')
                    for val in filter_def[u'values']:
                        gf1 = etree.Element(u'groupfilter')
                        gf1.set(u'function', u'member')
                        gf1.set(u'level', filter_def[u'column_name'])
                        # String types need &quot; , ints do not
                        if isinstance(val, basestring):
                            gf1.set(u'member', quoteattr(val))
                        else:
                            gf1.set(u'member', unicode(val))
                        gf.append(gf1)
                    f.append(gf)
            return_array.append(f)
        return return_array

    def generate_datasource_filters_section(self):
        filters = self.generate_filters(self.datasource_filters)
        filters_array = []
        for f in filters:
            filters_array.append(f)
        return filters_array

    def generate_cols_map_section(self):
        if len(self.column_mapping) == 0:
            return False
        c = etree.Element(u"cols")
        for key in self.column_mapping:
            m = etree.Element(u"map")
            m.set(u"key", u"[{}]".format(key))
            m.set(u"value", self.column_mapping[key])
            c.append(m)
        self.ds_xml.append(c)

    @staticmethod
    def generate_aliases_tag():
        # For whatever reason, the aliases tag does not contain the columns, but it always precedes it
        a = etree.Element(u"aliases")
        a.set(u"enabled", u"yes")
        return a

    def generate_aliases_column_section(self):
        column_aliases_array = []

        # Now to put in each column tag
        for column_alias in self.column_aliases:
            c = etree.Element(u"column")
            # Name is the Tableau Field Alias, always surrounded by brackets SQL Server style
            c.set(u"name", u"[{}]".format(column_alias))
            if self.column_aliases[column_alias][u"datatype"] is not None:
                c.set(u"datatype", self.column_aliases[column_alias][u"datatype"])
            if self.column_aliases[column_alias][u"caption"] is not None:
                c.set(u"caption", self.column_aliases[column_alias][u"caption"])
            if self.column_aliases[column_alias][u"role"] is not None:
                c.set(u"role", self.column_aliases[column_alias][u"role"])
            if self.column_aliases[column_alias][u"type"] is not None:
                c.set(u"type", self.column_aliases[column_alias][u"type"])
            if self.column_aliases[column_alias][u'calculation'] is not None:
                calc = etree.Element(u'calculation')
                calc.set(u'class', u'tableau')
                # quoteattr adds an extra real set of quotes around the string, which needs to be sliced out
                calc.set(u'formula', quoteattr(self.column_aliases[column_alias][u'calculation'])[1:-1])
                c.append(calc)
            column_aliases_array.append(c)
        return column_aliases_array

    def generate_column_instances_section(self):
        column_instances_array = []
        for column_instance in self.column_instances:
            ci = etree.Element(u'column-instance')
            ci.set(u'column', column_instance[u'column'])
            ci.set(u'derivation', u'None')
            ci.set(u'name', column_instance[u'name'])
            ci.set(u'pivot', u'key')
            ci.set(u'type', column_instance[u'type'])
            column_instances_array.append(ci)
        return column_instances_array

    def generate_extract_section(self):
        # Short circuit if no extract had been set
        if self.tde_filename is None:
            self.log('No tde_filename, no extract being added')
            return False
        self.log(u'Importing the Tableau SDK to build the extract')

        # Import only if necessary
        self.log(u'Building the extract Element object')
        from tde_file_generator import TDEFileGenerator
        e = etree.Element(u'extract')
        e.set(u'count', u'-1')
        e.set(u'enabled', u'true')
        e.set(u'units', u'records')

        c = etree.Element(u'connection')
        c.set(u'class', u'dataengine')
        c.set(u'dbname', u'Data/Datasources/{}'.format(self.tde_filename))
        c.set(u'schema', u'Extract')
        c.set(u'tablename', u'Extract')
        right_now = datetime.datetime.now()
        pretty_date = right_now.strftime("%m/%d/%Y %H:%M:%S %p")
        c.set(u'update-time', pretty_date)

        r = etree.Element(u'relation')
        r.set(u"name", u"Extract")
        r.set(u"table", u"[Extract].[Extract]")
        r.set(u"type", u"table")
        c.append(r)

        calcs = etree.Element(u"calculations")
        calc = etree.Element(u"calculation")
        calc.set(u"column", u"[Number of Records]")
        calc.set(u"formula", u"1")
        calcs.append(calc)
        c.append(calcs)

        ref = etree.Element(u'refresh')
        if self.incremental_refresh_field is not None:
            ref.set(u"increment-key", self.incremental_refresh_field)
            ref.set(u"incremental-updates", u'true')
        elif self.incremental_refresh_field is None:
            ref.set(u"increment-key", u"")
            ref.set(u"incremental-updates", u'false')

        c.append(ref)

        e.append(c)

        tde_columns = {}
        self.log(u'Creating the extract filters')
        if len(self.extract_filters) > 0:
            filters = self.generate_filters(self.extract_filters)
            for f in filters:
                e.append(f)
            # Any column in the extract filters needs to exist in the TDE file itself
            if len(self.extract_filters) > 0:
                for f in self.extract_filters:
                    # Check to see if column_name is actually an instance
                    field_name = f[u'column_name']
                    for col_instance in self.column_instances:
                        if field_name == col_instance[u'name']:
                            field_name = col_instance[u'column']
                    # Simple heuristic for determining type from the first value in the values array
                    if f[u'type'] == u'categorical':
                        first_value = f[u'values'][0]
                        if isinstance(first_value, basestring):
                            filter_column_tableau_type = 'str'
                        else:
                            filter_column_tableau_type = 'int'
                    elif f[u'type'] == u'relative-date':
                        filter_column_tableau_type = 'datetime'
                    elif f[u'type'] == u'quantitative':
                        # Time values passed in with strings
                        if isinstance(f[u'max'], basestring) or isinstance(f[u'min'], basestring):
                            filter_column_tableau_type = 'datetime'
                        else:
                            filter_column_tableau_type = 'int'
                    else:
                        raise InvalidOptionException('{} is not a valid type'.format(f[u'type']))
                    tde_columns[field_name[1:-1]] = filter_column_tableau_type
        else:
            self.log(u'Creating TDE with only one field, "Generic Field", of string type')
            tde_columns[u'Generic Field'] = 'str'

        self.log(u'Using the Extract SDK to build an empty extract file with the right definition')
        tde_file_generator = TDEFileGenerator(self.logger)
        tde_file_generator.set_table_definition(tde_columns)
        tde_file_generator.create_tde(self.tde_filename)
        return e

    def get_xml_string(self):
        self.generate_relation_section()
        self.generate_cols_map_section()

        # Column Aliases
        cas = self.generate_aliases_column_section()
        # Need Aliases tag if there are any column tags
        if len(cas) > 0:
            self.ds_xml.append(self.generate_aliases_tag())
        for c in cas:
            self.log(u'Appending the column alias XML')
            self.ds_xml.append(c)
        # Column Instances
        cis = self.generate_column_instances_section()
        for ci in cis:
            self.log(u'Appending the column-instances XML')
            self.ds_xml.append(ci)
        # Data Source Filters
        dsf = self.generate_datasource_filters_section()
        for f in dsf:
            self.log(u'Appending the datasource filters XML')
            self.ds_xml.append(f)
        # Extract
        e = self.generate_extract_section()
        if e is not False:
            self.log(u'Appending the extract XML')
            self.ds_xml.append(e)

        xmlstring = etree.tostring(self.ds_xml, pretty_print=True, xml_declaration=True, encoding='utf-8')
        self.log(xmlstring)
        return xmlstring

    def save_file(self, filename_no_extension, save_to_directory):
        self.start_log_block()
        file_extension = u'.tds'
        if self.tde_filename is not None:
            file_extension = u'.tdsx'
        try:
            tds_filename = filename_no_extension + u'.tds'
            lh = open(save_to_directory + tds_filename, 'wb')
            lh.write(self.get_xml_string())
            lh.close()

            if file_extension == u'.tdsx':
                zf = zipfile.ZipFile(save_to_directory + filename_no_extension + u'.tdsx', 'w')
                zf.write(save_to_directory + tds_filename, u'/{}'.format(tds_filename))
                # Delete temporary TDS at some point
                zf.write(self.tde_filename, u'/Data/Datasources/{}'.format(self.tde_filename))
                zf.close()
                # Remove the temp tde_file that is created
                os.remove(self.tde_filename)
        except IOError:
            self.log(u"Error: File '{} cannot be opened to write to".format(filename_no_extension + file_extension))
            self.end_log_block()
            raise

    def add_extract(self, tde_filename, incremental_refresh_field=None):
        self.tde_filename = tde_filename
        self.incremental_refresh_field = incremental_refresh_field


class TableauParametersGenerator(TableauBase):
    def __init__(self, logger_obj):
        super(self.__class__, self).__init__()
        self.logger = logger_obj
        self.nsmap = {u"user": u'http://www.tableausoftware.com/xml/user'}
        self.ds_xml = etree.Element(u"datasource")
        self.ds_xml.set(u'name', u'Parameters')
        # Initialization of the datasource
        self.ds_xml.set(u'hasconnection', u'false')
        self.ds_xml.set(u'inline', u'true')
        a = etree.Element(u'aliases')
        a.set(u'enabled', u'yes')
        self.ds_xml.append(a)

        self.parameters = []

    def add_parameter(self, name, datatype, allowable_values, current_value, values_list=None, range_dict=None):
        if datatype.lower() not in [u'string', u'integer', u'datetime', u'date', u'real', u'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))
        if allowable_values not in [u'all', u'list', u'range']:
            raise InvalidOptionException("{} is not valid allowable_values option. Only 'all', 'list' or 'range'")

        # range_dict = { min: None, max: None, step_size: None, period_type: None}

        param_dict = {
                        u'allowable_values': allowable_values,
                        u'datatype': datatype,
                        u'current_value': current_value,
                        u'values_list': values_list,
                        u'range_dict': range_dict,
                        u'caption': name
        }
        self.parameters.append(param_dict)

    @staticmethod
    def create_parameter_column(param_number, param_dict):
        c = etree.Element(u"column")
        c.set(u'caption', param_dict[u'caption'])
        c.set(u'name', u'[Parameter {}]'.format(unicode(param_number)))
        c.set(u'param-domain-type', param_dict[u'allowable_values'])
        if param_dict[u'datatype'] in [u'integer', u'real']:
            c.set(u'type', u'quantitative')
        else:
            c.set(u'type', u'nominal')
        c.set(u'role', u'measure')
        c.set(u'datatype', param_dict[u'datatype'])

        # Range
        if param_dict[u'allowable_values'] == u'range':
            r = etree.Element(u'range')
            if param_dict[u'range_dict'][u'max'] is not None:
                r.set(u'max', unicode(param_dict[u'range_dict'][u'max']))
            if param_dict[u'range_dict'][u'min'] is not None:
                r.set(u'min', unicode(param_dict[u'range_dict'][u'min']))
            if param_dict[u'range_dict'][u'step_size'] is not None:
                r.set(u'granularity', unicode(param_dict[u'range_dict'][u'step_size']))
            if param_dict[u'range_dict'][u'period_type'] is not None:
                r.set(u'period-type', unicode(param_dict[u'range_dict'][u'period_type']))
            c.append(r)

        # List
        aliases = None
        if param_dict[u'allowable_values'] == u'list':
            members = etree.Element(u'members')

            for value_pair in param_dict[u'values_list']:
                for value in value_pair:
                    member = etree.Element(u'member')
                    member.set(u'value', unicode(value))
                    if value_pair[value] is not None:
                        if aliases is None:
                            aliases = etree.Element(u'aliases')
                        alias = etree.Element(u'alias')
                        alias.set(u'key', unicode(value))
                        alias.set(u'value', unicode(value_pair[value]))
                        member.set(u'alias', unicode(value_pair[value]))
                        aliases.append(alias)
                    members.append(member)
                if aliases is not None:
                    c.append(aliases)
                c.append(members)

        # If you have aliases, then need to put alias in the alias parameter, and real value in the value parameter
        if aliases is not None:
            c.set(u'alias', unicode(param_dict[u'current_value']))
            # Lookup the actual value of the alias
            for value_pair in param_dict[u'values_list']:
                for value in value_pair:
                    if value_pair[value] == param_dict[u'current_value']:
                        actual_value = value
        else:
            actual_value = param_dict[u'current_value']

        if isinstance(param_dict[u'current_value'], basestring) and param_dict[u'datatype'] not in [u'date', u'datetime']:
            c.set(u'value', quoteattr(actual_value))
        else:
            c.set(u'value', unicode(actual_value))

        calc = etree.Element(u'calculation')
        calc.set(u'class', u'tableau')
        if isinstance(param_dict[u'current_value'], basestring) and param_dict[u'datatype'] not in [u'date', u'datetime']:
            calc.set(u'formula', quoteattr(actual_value))
        else:
            calc.set(u'formula', unicode(actual_value))
        c.append(calc)

        return c

    def get_xml_string(self):
        i = 1
        for parameter in self.parameters:
            c = self.create_parameter_column(i, parameter)
            self.ds_xml.append(c)
            i += 1

        xmlstring = etree.tostring(self.ds_xml, pretty_print=True, xml_declaration=False, encoding='utf-8')
        self.log(xmlstring)
        return xmlstring
