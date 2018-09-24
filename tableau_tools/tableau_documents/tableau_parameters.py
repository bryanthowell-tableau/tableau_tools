from ..tableau_base import *
from .tableau_document import TableauDocument

import xml.etree.cElementTree as etree
from ..tableau_exceptions import *

from xml.sax.saxutils import quoteattr, unescape
import datetime
import collections


class TableauParameters(TableauDocument):
    def __init__(self, datasource_xml=None, logger_obj=None):
        """
        :type datasource_xml: etree.Element
        :type logger_obj: Logger
        """
        TableauDocument.__init__(self)
        self.logger = logger_obj
        self._parameters = []  # type: list[TableauParameter]
        self._highest_param_num = 1
        self.log('Initializing TableauParameters object')
        self._document_type = 'parameters'
        # Initialize new Parameters datasource if existing xml is not passed in

        if datasource_xml is None:
            self.log('No Parameter XML passed in, building from scratch')
            self.ds_xml = etree.Element("datasource")
            self.ds_xml.set('name', 'Parameters')
            # Initialization of the datasource
            self.ds_xml.set('hasconnection', 'false')
            self.ds_xml.set('inline', 'true')
            a = etree.Element('aliases')
            a.set('enabled', 'yes')
            self.ds_xml.append(a)
        else:
            self.log('Parameter XML passed in, finding essential characteristics')
            self.ds_xml = datasource_xml
            params_xml = self.ds_xml.findall('./column')
            for column in params_xml:
                alias = column.get('caption')
                internal_name = column.get('name')  # type: unicode
                # Parameters are all given internal name [Parameter #], unless they are copies where they
                # end with (copy) h/t Jeff James for discovering
                if internal_name.find('Parameter') != -1 and internal_name.find('(copy)') == -1:
                    param_num = int(internal_name.split(" ")[1][0])
                    # Move up the highest_param_num counter for when you add new ones
                    if param_num > self._highest_param_num:
                        self._highest_param_num = param_num

                p = TableauParameter(parameter_xml=column, logger_obj=self.logger)
                self._parameters.append(p)

    def get_datasource_xml(self):
        self.start_log_block()
        xmlstring = etree.tostring(self.ds_xml)
        self.end_log_block()
        return xmlstring

    def get_parameter_by_name(self, parameter_name):
        """
        :type parameter_name: unicode
        :rtype: TableauParameter
        """
        for p in self._parameters:
            if p.name == parameter_name:
                return p
        else:
            raise NoMatchFoundException('No parameter named {}'.format(parameter_name))

    def create_new_parameter(self, name=None, datatype=None, current_value=None):
        """
        :rtype: TableauParameter
        """
        # Need to check existing Parameter numbers
        self._highest_param_num += 1
        p = TableauParameter(parameter_xml=None, parameter_number=self._highest_param_num, logger_obj=self.logger,
                             name=name, datatype=datatype, current_value=current_value)

        return p

    def add_parameter(self, parameter):
        """
        :type parameter: TableauParameter
        :return:
        """
        if isinstance(parameter, TableauParameter) is not True:
            raise InvalidOptionException('parameter must be a TableauParameter object')
        self._parameters[parameter.name] = parameter

    def delete_parameter_by_name(self, parameter_name):
        if self._parameters.get(parameter_name) is not None:
            param_xml = self.ds_xml.find('./column[@caption="{}"]'.format(parameter_name))
            del self._parameters[parameter_name]
            # Might be unnecessary but who am I to say what won't happen
            if param_xml is not None:
                self.ds_xml.remove(param_xml)


class TableauParameter(TableauBase):
    def __init__(self, parameter_xml=None, parameter_number=None, logger_obj=None, name=None, datatype=None,
                 current_value=None):
        """
        :type parameter_xml: etree.Element
        :type logger_obj: Logger
        """
        TableauBase.__init__(self)
        self.logger = logger_obj
        self._aliases = False
        self._values_list = None

        if parameter_xml is not None:
            self.p_xml = parameter_xml

            # Grab any aliases and members and generate a list of the values

        # Initialization of the column element
        else:
            if parameter_number is None:
                raise InvalidOptionException('Must pass a parameter_number if creating a new Parameter')
            self.p_xml = etree.Element("column")
            self.p_xml.set('name', '[Parameter {}]'.format(str(parameter_number)))
            self.p_xml.set('role', 'measure')
            # Set allowable_values to all by default
            self.p_xml.set('param-domain-type', 'all')
            if name is not None:
                self.name = name
            if datatype is not None:
                self.datatype = datatype
            if current_value is not None:
                self.current_value = current_value

    @property
    def name(self):
        return self.p_xml.get('caption')

    @name.setter
    def name(self, name):
        if self.p_xml.get('caption') is not None:
            self.p_xml.attrib['caption'] = name
        else:
            self.p_xml.set('caption', name)

    @property
    def datatype(self):
        return self.p_xml.get('datatype')

    @datatype.setter
    def datatype(self, datatype):
        if datatype.lower() not in ['string', 'integer', 'datetime', 'date', 'real', 'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))
        if self.p_xml.get("datatype") is not None:
            self.p_xml.attrib["datatype"] = datatype
            if datatype in ['integer', 'real']:
                self.p_xml.attrib['type'] = 'quantitative'
            else:
                self.p_xml.attrib['type'] = 'nominal'

        else:
            self.p_xml.set('datatype', datatype)
            if datatype in ['integer', 'real']:
                self.p_xml.set('type', 'quantitative')
            else:
                self.p_xml.set('type', 'nominal')

    @property
    def allowable_values(self):
        return self.p_xml.get('param-domain-type')

    def set_allowable_values_to_range(self, minimum=None, maximum=None, step_size=None, period_type=None):
        # Automatically switch to a range param-domain-type and clean up list version
        if self.p_xml.get('param-domain-type') == 'list':
            a = self.p_xml.find('./aliases')
            if a is not None:
                self.p_xml.remove(a)

            m = self.p_xml.find('./members')
            if m is not None:
                self.p_xml.remove(m)

            c = self.p_xml.find('./calculation')
            if c is not None:
                self.p_xml.remove(c)

        self.p_xml.set('param-domain-type', 'range')
        # See if a range already exists, otherwise create it
        r = self.p_xml.find('./range', self.ns_map)
        if r is None:
            r = etree.Element('range')
            self.p_xml.append(r)

        # Set any new values that come through
        if maximum is not None:
            r.set('max', str(maximum))
        if minimum is not None:
            r.set('min', str(minimum))
        if step_size is not None:
            r.set('granularity', str(step_size))
        if period_type is not None:
            r.set('period-type', str(period_type))

    def set_allowable_values_to_list(self, list_value_display_as_pairs):
        """
        :param list_value_display_as_pairs: To maintain ordering, pass in the values as a list of {value : display_as } dict elements
        :type list_value_display_as_pairs: list[dict]
        :return:
        """
        # Automatically switch to a range param-domain-type and clean up list version
        if self.p_xml.get('param-domain-type') == 'range':
            r = self.p_xml.find('./range')
            if r is not None:
                self.p_xml.remove(r)
            # Clear the preset value if a RANGE previously, leave it if it was already a list
            c = self.p_xml.find('./calculation')
            if c is not None:
                self.p_xml.remove(c)

        self.p_xml.set('param-domain-type', 'list')
        # Store the values list for lookups later

        # Remove existing members and aliases
        a = self.p_xml.find('./aliases')
        if a is not None:
            self.p_xml.remove(a)

        m = self.p_xml.find('./members')
        if m is not None:
            self.p_xml.remove(m)

        aliases = None
        members = etree.Element('members')

        for value_pair in list_value_display_as_pairs:
            for value in value_pair:
                member = etree.Element('member')
                member.set('value', str(value))
                if value_pair[value] is not None:
                    if aliases is None:
                        aliases = etree.Element('aliases')
                    alias = etree.Element('alias')
                    alias.set('key', str(value))
                    alias.set('value', str(value_pair[value]))
                    member.set('alias', str(value_pair[value]))
                    aliases.append(alias)
                members.append(member)
        if aliases is not None:
            self.p_xml.append(aliases)
            self._aliases = True
        else:
            self._aliases = False
        self.p_xml.append(members)

    def set_allowable_values_to_all(self):

        # Clear anything that range or list would have

        r = self.p_xml.find('./range')
        if r is not None:
            self.p_xml.remove(r)

        a = self.p_xml.find('./aliases')
        if a is not None:
            self.p_xml.remove(a)

        m = self.p_xml.find('./members')
        if m is not None:
            self.p_xml.remove(m)

        self.p_xml.set('param-domain-type', 'all')

    @property
    def current_value(self):
        # Returns the alias if one exists
        if self.p_xml.get('alias') is None:
            return self.p_xml.get('value')
        else:
            return self.p_xml.get('alias')

    @current_value.setter
    def current_value(self, current_value):
        # The set value is both in the column tag and has a separate calculation tag

        # If there is an alias, have to grab the real value
        actual_value = current_value
        if self._aliases is True:
            self.p_xml.set('alias', str(current_value))
            # Lookup the actual value of the alias
            for value_pair in self._values_list:
                for value in value_pair:
                    if value_pair[value] == current_value:
                        actual_value = value

        # Why there have to be a calculation tag as well? I don't know, but there does
        calc = etree.Element('calculation')
        calc.set('class', 'tableau')
        if isinstance(current_value, str) and self.datatype not in ['date', 'datetime']:
            self.p_xml.set('value', quoteattr(actual_value))
            calc.set('formula', quoteattr(actual_value))
        elif self.datatype in ['date', 'datetime']:
            if isinstance(current_value, datetime.date) or isinstance(current_value, datetime.datetime):
                time_str = "#{}#".format(current_value.strftime('%Y-%m-%d %H-%M-%S'))
                calc.set('formula', time_str)
            else:
                if not (current_value[0] == '#' and current_value[-1] == '#'):
                    raise InvalidOptionException('Time and Datetime strings must start and end with #')
                else:
                    calc.set('formula', str(current_value))
        else:
            self.p_xml.set('value', str(actual_value))
            calc.set('formula', str(actual_value))

        self.p_xml.append(calc)
