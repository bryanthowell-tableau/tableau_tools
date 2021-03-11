# -*- coding: utf-8 -*-

from tableausdk import *
from tableausdk.HyperExtract import *
from tableau_tools.logging_methods import LoggingMethods


class HyperFileGenerator(LoggingMethods):
    def __init__(self, logger_obj=None):
        super(self.__class__, self).__init__()
        self.logger = logger_obj
        self.field_setter_map = {
            Type.BOOLEAN: lambda row, col_num, value: row.setBoolean(col_num, value.lower() == "true"),
            Type.INTEGER: lambda row, col_num, value: row.setInteger(col_num, int(value)),
            Type.DOUBLE: lambda row, col_num, value: row.setDouble(col_num, float(value)),
            Type.UNICODE_STRING: lambda row, col_num, value: row.setString(col_num, value),
            Type.CHAR_STRING: lambda row, col_num, value: row.setCharString(col_num, value),
            Type.DATE: lambda row, col_num, value: self.set_date(row, col_num, value),
            Type.DATETIME: lambda row, col_num, value: self.set_date_time(row, col_num, value)
        }
        # Simple mapping of the string name of the Python type objects (accessed by __name__ property) to the TDE types
        self.python_type_map = {
                                'float': Type.DOUBLE,
                                'int': Type.INTEGER,
                                'unicode': Type.UNICODE_STRING,
                                'str': Type.CHAR_STRING,
                                'datetime': Type.DATETIME,
                                'boolean': Type.BOOLEAN,
                                'date': Type.DATE
                                }

        self.table_definition = None
        self.tde_object = None

    @staticmethod
    def set_date(row, col_num, value):
        # d = datetime.datetime.strptime(value, "%Y-%m-%d")
        d = value
        row.setDate(col_num, d.year, d.month, d.day)

    @staticmethod
    def set_date_time(row, col_num, value):
        # if( value.find(".") != -1) :
        #        d = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
        # else :
        #        d = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        d = value
        row.setDateTime(col_num, d.year, d.month, d.day, d.hour, d.minute, d.second, d.microsecond / 100)

    def set_table_definition(self, column_name_type_dict, collation=Collation.EN_US):
        self.table_definition = TableDefinition()

        # Assuming EN_US, should be made optional
        self.table_definition.setDefaultCollation(collation)

        for col in column_name_type_dict:
            self.table_definition.addColumn(col, self.python_type_map[column_name_type_dict[col]])
        return self.table_definition

    def create_table_definition_from_pyodbc_cursor(self, pydobc_cursor, collation=Collation.EN_US):
        self.table_definition = TableDefinition()
        # Assuming EN_US, should be made optional
        self.table_definition.setDefaultCollation(collation)

        # cursor.description is collection to find information about the returned columns
        # for example, the column names and the datatypes.

        for col in pydobc_cursor.description:
            # [0] is the column name string, [1] is the python type object
            print((col[0].decode('utf-8')))
            self.log('Adding {} {}'.format(col[0], col[1].__name__))
            # Second item is a Python Type object, to get the actual name as a string for comparison, have to use __name__ property
            # Check against the type maps, drop the column if we don't understand the types
            if col[1].__name__ in self.python_type_map:
                self.table_definition.addColumn(col[0], self.python_type_map[col[1].__name__])
            else:
                self.log('Skipped column {}, {}'.format(col[0], col[1].__name__))
        return self.table_definition

#    def create_table_definition_from_pandas_dataframe(self, pandas_dataframe, collation=Collation.EN_US):


    def create_extract(self, tde_filename, append=False, table_name='Extract', pyodbc_cursor=None):
        try:
            # Using "with" handles closing the TDE correctly

            with Extract("{}".format(tde_filename)) as extract:
                self.tde_object = None
                row_count = 0
                # Create the Extract object (or set it for updating) if there are actually results
                if not extract.hasTable(table_name):
                    # Table does not exist; create it
                    self.log('Creating Extract with table definition')
                    self.tde_object = extract.addTable(table_name, self.table_definition)
                else:
                    # Open an existing table to add more rows
                    if append is True:
                        self.tde_object = extract.openTable(table_name)
                    else:
                        self.log("Output file '{}' already exists.".format(tde_filename))
                        self.log("Append mode is off, please delete file and then rerun...")
                        sys.exit()
                # This is if you actually have data to put into the extract. Implement later
                if pyodbc_cursor is not None:
                    row_count = 0
                    for db_row in pyodbc_cursor:
                        tde_row = Row(self.table_definition)
                        col_no = 0
                        for field in db_row:
                            # Possible for database to have types that do not map, we skip them
                            if pyodbc_cursor.description[col_no][1].__name__ in self.python_type_map:
                                if field == "" or field is None:
                                    tde_row.setNull(col_no)
                                else:
                                    # From any given row from the cursor object, we can use the cursor_description collection to find information
                                    # for example, the column names and the datatypes. [0] is the column name string, [1] is the python type object. Mirrors cursor.description on the Row level
                                    # Second item is a Python Type object, to get the actual name as a string for comparison, have to use __name__ property
                                    self.field_setter_map[self.python_type_map[pyodbc_cursor.description[col_no][1].__name__ ]](tde_row, col_no, field)
                                    col_no += 1
                        self.tde_object.insert(tde_row)
                        row_count += 1
                    self.log("Hyper creation complete, {} rows inserted".format(row_count))
                    #if len(skipped_cols) > 0:
                    #   self.log(u"The following columns were skipped due to datatypes that were not recognized: ")
                    #   self.log(unicode(skipped_cols))

        except TableauException as e:
            self.log('Tableau Hyper creation error:{}'.format(e))
            raise

