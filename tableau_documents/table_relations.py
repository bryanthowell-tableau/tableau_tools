import xml.etree.ElementTree as ET
from typing import Union, Any, Optional, List, Dict, Tuple
import random
from xml.sax.saxutils import quoteattr, unescape
import copy
import datetime

from ..tableau_exceptions import *
from ..tableau_rest_xml import TableauRestXml

# This represents the classic Tableau data connection window relations
# Allows for changes in JOINs, Stored Proc values, and Custom SQL
class TableRelations():
    #
    # Reading existing table relations
    #
    def __init__(self, relation_xml_obj: ET.Element):
        self.relation_xml_obj = relation_xml_obj
        self.main_table: ET.Element
        self.table_relations: List[ET.Element]
        self.join_relations = []
        #self.ns_map = {"user": 'http://www.tableausoftware.com/xml/user', 't': 'http://tableau.com/api'}
        #ET.register_namespace('t', self.ns_map['t'])
        self._read_existing_relations()

    def _read_existing_relations(self):
        # Test for single relation
        relation_type = self.relation_xml_obj.get('type')
        if relation_type != 'join':
                self.main_table = self.relation_xml_obj
                self.table_relations = [self.relation_xml_obj, ]

        else:
            table_relations = self.relation_xml_obj.findall('.//relation', TableauRestXml.ns_map)
            final_table_relations = []
            # ElementTree doesn't implement the != operator, so have to find all then iterate through to exclude
            # the JOINs to only get the tables, stored-procs and Custom SQLs
            for t in table_relations:
                if t.get('type') != 'join':
                    final_table_relations.append(t)
            self.main_table = final_table_relations[0]
            self.table_relations = final_table_relations

        # Read any parameters that a stored-proc might have
        if self.main_table.get('type') == 'stored-proc':
            self._stored_proc_parameters_xml = self.main_table.find('.//actual-parameters')

    @property
    def main_custom_sql(self) -> str:
        if self.main_table.get('type') == 'stored-proc':
            return self.main_table.text
        else:
            raise InvalidOptionException('Data Source does not have Custom SQL defined')

    @main_custom_sql.setter
    def main_custom_sql(self, new_custom_sql: str):
        if self.main_table.get('type') == 'stored-proc':
            self.main_table.text = new_custom_sql
        else:
            raise InvalidOptionException('Data Source does not have Custom SQL defined')

    @property
    def main_table_name(self) -> str:
        if self.main_table.get('type') == 'table':
            return self.main_table.get('table')
        else:
            raise InvalidOptionException('Data Source main relation is not a database table (or view). Possibly Custom SQL or Stored Procedure')

    @main_table_name.setter
    def main_table_name(self, new_table_name:str):
        if self.main_table.get('type') == 'table':
            self.main_table.set('table', new_table_name)
        else:
            raise InvalidOptionException(
                'Data Source main relation is not a database table (or view). Possibly Custom SQL or Stored Procedure')

    #
    # For creating new table relations
    #
    def set_first_table(self, db_table_name: str, table_alias: str, connection: Optional[str] = None,
                        extract: bool = False):
        self.ds_generator = True
        # Grab the original connection name
        if self.main_table is not None and connection is None:
            connection = self.main_table.get('connection')
        self.main_table = self.create_table_relation(db_table_name, table_alias, connection=connection,
                                                              extract=extract)

    def set_first_custom_sql(self, custom_sql: str, table_alias: str, connection: Optional[str] = None):
        self.ds_generator = True
        if self.main_table is not None and connection is None:
            connection = self.main_table.get('connection')
        self.main_table = self.create_custom_sql_relation(custom_sql, table_alias, connection=connection)

    def set_stored_proc(self, stored_proc_name: str, connection: Optional[str] = None):
        self.ds_generator = True
        if self.main_table is not None and connection is None:
            connection = self.main_table.get('connection')
        self.main_table = self.create_stored_proc_relation(stored_proc_name)

    def get_stored_proc_parameter_value_by_name(self, parameter_name: str) -> str:
        if self._stored_proc_parameters_xml is None:
            raise NoResultsException('There are no parameters set for this stored proc (or it is not a stored proc)')
        param = self._stored_proc_parameters_xml.find('../column[@name="{}"]'.format(parameter_name))
        if param is None:
            raise NoMatchFoundException('Could not find Stored Proc parameter with name {}'.format(parameter_name))
        else:
            value = param.get('value')

            # Maybe add deserializing of the dates and datetimes eventally?

            # Remove the quoting and any escaping
            if value[0] == '"' and value[-1] == '"':
                return unescape(value[1:-1])
            else:
                return unescape(value)

    def set_stored_proc_parameter_value_by_name(self, parameter_name: str, parameter_value: str):
        # Create if there is none
        if self._stored_proc_parameters_xml is None:
            self._stored_proc_parameters_xml = ET.Element('actual-parameters')
        # Find parameter with that name (if exists)
        param = self._stored_proc_parameters_xml.find('.//column[@name="{}"]'.format(parameter_name), TableauRestXml.ns_map)

        if param is None:
            # create_stored... already converts to correct quoting
            new_param = self.create_stored_proc_parameter(parameter_name, parameter_value)

            self._stored_proc_parameters_xml.append(new_param)
        else:
            if isinstance(parameter_value, str):
                final_val = quoteattr(parameter_value)
            elif isinstance(parameter_value, datetime.date) or isinstance(parameter_value, datetime.datetime):
                    time_str = "#{}#".format(parameter_value.strftime('%Y-%m-%d %H-%M-%S'))
                    final_val = time_str
            else:
                final_val = str(parameter_value)
            param.set('value', final_val)

    @staticmethod
    def create_stored_proc_parameter(parameter_name: str, parameter_value: Any) -> ET.Element:
        c = ET.Element('column')
        # Check to see if this varies at all depending on type or whatever
        c.set('ordinal', '1')
        if parameter_name[0] != '@':
            parameter_name = "@{}".format(parameter_name)
        c.set('name', parameter_name)
        if isinstance(parameter_value, str):
            c.set('value', quoteattr(parameter_value))
        elif isinstance(parameter_value, datetime.date) or isinstance(parameter_value, datetime.datetime):
            time_str = "#{}#".format(parameter_value.strftime('%Y-%m-%d %H-%M-%S'))
            c.set('value', time_str)
        else:
            c.set('value', str(parameter_value))
        return c

    @staticmethod
    def create_random_calculation_name() -> str:
        n = 19
        range_start = 10 ** (n - 1)
        range_end = (10 ** n) - 1
        random_digits = random.randint(range_start, range_end)
        return 'Calculation_{}'.format(str(random_digits))

    @staticmethod
    def create_table_relation(db_table_name: str, table_alias: str, connection: Optional[str] = None,
                              extract: bool = False) -> ET.Element:
        r = ET.Element("relation")
        r.set('name', table_alias)
        if extract is True:
            r.set("table", "[Extract].[{}]".format(db_table_name))
        else:
            r.set("table", "[{}]".format(db_table_name))
        r.set("type", "table")
        if connection is not None:
            r.set('connection', connection)
        return r

    @staticmethod
    def create_custom_sql_relation(custom_sql: str, table_alias: str, connection: Optional[str] = None) -> ET.Element:
        r = ET.Element("relation")
        r.set('name', table_alias)
        r.text = custom_sql
        r.set("type", "text")
        if connection is not None:
            r.set('connection', connection)
        return r

    # UNFINISHED, NEEDS TESTING TO COMPLETE
    @staticmethod
    def create_stored_proc_relation(stored_proc_name: str, connection: Optional[str] = None, actual_parameters=None):
        r = ET.Element("relation")
        r.set('name', stored_proc_name)
        r.set("type", "stored-proc")
        if connection is not None:
            r.set('connection', connection)
        if actual_parameters is not None:
            r.append(actual_parameters)
        return r

    # on_clauses = [ { left_table_alias : , left_field : , operator : right_table_alias : , right_field : ,  },]
    @staticmethod
    def define_join_on_clause(left_table_alias, left_field, operator, right_table_alias, right_field):
        return {"left_table_alias": left_table_alias,
                "left_field": left_field,
                "operator": operator,
                "right_table_alias": right_table_alias,
                "right_field": right_field
                }

    def join_table(self, join_type: str, db_table_name: str, table_alias: str, join_on_clauses: List[Dict],
                   custom_sql: Optional[str] = None):
        full_join_desc = {"join_type": join_type.lower(),
                          "db_table_name": db_table_name,
                          "table_alias": table_alias,
                          "on_clauses": join_on_clauses,
                          "custom_sql": custom_sql}
        self.join_relations.append(full_join_desc)

    def generate_relation_section(self, connection_name: Optional[str] = None) -> ET.Element:
        # Because of the strange way that the interior definition is the last on, you need to work inside out
        # "Middle-out" as Silicon Valley suggests.
        # Generate the actual JOINs
        #if self.relation_xml_obj is not None:
        #    self.relation_xml_obj.clear()
        #else:
        rel_xml_obj = ET.Element("relation")
        # There's only a single main relation with only one table

        if len(self.join_relations) == 0:
            for item in list(self.main_table.items()):
                rel_xml_obj.set(item[0], item[1])
            if self.main_table.text is not None:
                rel_xml_obj.text = self.main_table.text

        else:
            prev_relation = None

            # We go through each relation, build the whole thing, then append it to the previous relation, then make
            # that the new prev_relationship. Something like recursion
            #print(self.join_relations)
            for join_desc in self.join_relations:

                r = ET.Element("relation")
                r.set("join", join_desc["join_type"])
                r.set("type", "join")
                if len(join_desc["on_clauses"]) == 0:
                    raise InvalidOptionException("Join clause must have at least one ON clause describing relation")
                else:
                    and_expression = None
                    if len(join_desc["on_clauses"]) > 1:
                        and_expression = ET.Element("expression")
                        and_expression.set("op", 'AND')
                    for on_clause in join_desc["on_clauses"]:
                        c = ET.Element("clause")
                        c.set("type", "join")
                        e = ET.Element("expression")
                        e.set("op", on_clause["operator"])

                        e_field1 = ET.Element("expression")
                        e_field1_name = '[{}].[{}]'.format(on_clause["left_table_alias"],
                                                            on_clause["left_field"])
                        e_field1.set("op", e_field1_name)
                        e.append(e_field1)

                        e_field2 = ET.Element("expression")
                        e_field2_name = '[{}].[{}]'.format(on_clause["right_table_alias"],
                                                            on_clause["right_field"])
                        e_field2.set("op", e_field2_name)
                        e.append(e_field2)
                        if and_expression is not None:
                            and_expression.append(e)
                        else:
                            and_expression = e
                c.append(and_expression)
                r.append(c)
                if prev_relation is not None:
                    r.append(prev_relation)

                if join_desc["custom_sql"] is None:
                    # Append the main table first (not sure this works for more deep hierarchies, but let's see
                    main_rel_xml_obj = ET.Element('relation')
                    for item in list(self.main_table.items()):
                        main_rel_xml_obj.set(item[0], item[1])
                    if self.main_table.text is not None:
                        main_rel_xml_obj.text = self.main_table.text
                    main_rel_xml_obj.set('connection', connection_name)
                    r.append(main_rel_xml_obj)

                    new_table_rel = self.create_table_relation(join_desc["db_table_name"],
                                                               join_desc["table_alias"], connection=connection_name)
                else:
                    new_table_rel = self.create_custom_sql_relation(join_desc['custom_sql'],
                                                                    join_desc['table_alias'], connection=connection_name)
                r.append(new_table_rel)
                prev_relation = r
                #prev_relation = copy.deepcopy(r)
                #rel_xml_obj.append(copy.deepcopy(r))
            rel_xml_obj = copy.deepcopy(prev_relation)
        return rel_xml_obj