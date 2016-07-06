# -*- coding: utf-8 -*-
from tableau_tools import *
from tableau_tools.tableau_documents import *

logger = Logger(u'datasource_generation_example.log')
file_dir = u'C:\\Users\\{}\\Documents\\My Tableau Repository\\Datasources\\'

# Simple data source connection to a single table on a Postgres DB
ds = TableauDatasourceGenerator(u'postgres', u'My DS', u'localhost', u'demo', logger)
ds.add_first_table(u'agency_sales', u'Super Store')
ds.save_file(u"one_table", file_dir)

# Adding in a table JOIN
ds2 = TableauDatasourceGenerator(u'postgres', u'My DS', u'localhost', u'demo', logger)
ds2.add_first_table(u'agency_sales', u'Super Store')
# Define each JOIN ON clause first, then JOIN the table. On clauses should be put into a list, to handle multiple keys
join_on = ds2.define_join_on_clause(u'Super Store', u'region', u'=', u'Entitled People', u'region')
ds2.join_table(u'Inner', u'superstore_entitlements', u'Entitled People', [join_on, ])
ds2.save_file(u"two_tables", file_dir)

# Single table, but several data source filters
ds3 = TableauDatasourceGenerator(u'postgres', u'My DS', u'localhost', u'demo', logger)
ds3.add_first_table(u'agency_sales', u'Super Store')
# Single inclusive filter
ds3.add_dimension_datasource_filter(u'category', [u'Furniture', ])
# Multiple inclusive filter
ds3.add_dimension_datasource_filter(u'region', [u'East', u'West'])
# Single exclusive filter
ds3.add_dimension_datasource_filter(u'city', [u'San Francisco', ], include_or_exclude=u'exclude')
# Multiple exclusive filter
ds3.add_dimension_datasource_filter(u'state', [u'Arkansas', u'Texas'], include_or_exclude=u'exclude')
# Numeric inclusive filter
ds3.add_dimension_datasource_filter(u'row_id', [2, 5, 10, 22])
# Numeric continuous greater than filter
ds3.add_continuous_datasource_filter(u'profit', min_value=20)
# Relative date filter
ds3.add_relative_date_datasource_filter(u'order_date', u'year', previous_next_current=u'previous', number_of_periods=4)

# Add a calculation (this one does row level security
calc_id = ds3.add_calculation(u'IIF([salesperson_user_id]=USERNAME(),1,0) ', u'Row Level Security',
                              u'dimension', u'discrete', u'integer')
# Create a data source filter that references the calculation
ds3.add_dimension_datasource_filter(calc_id, [1, ], custom_value_list=True)
ds3.save_file(u"one_table_many_ds_filters", file_dir)

# Create a data source with an extract
ds4 = TableauDatasourceGenerator(u'postgres', u'My DS', u'localhost', u'demo', logger)
ds4.add_first_table(u'agency_sales', u'Super Store')
# This is the name you want the TDE to have within the TDSX file. Not an external existing filename
ds4.add_extract(u'Datasource.tde')
ds4.add_dimension_extract_filter(u'region', [u'East', u'West'])
ds4.add_dimension_extract_filter(u'sub-category', [u'Paper', ], include_or_exclude=u'exclude')
ds4.add_continuous_extract_filter(u'order_date', u'2013-04-01', u'2014-04-23', date=True)
ds4.save_file(u'extract_datasource', file_dir)


# ds.add_column_alias('profit', 'Profit', 'measure', u'continuous', 'real')
#

# SQL Server source with Custom SQL and Initial SQL with Parameters
initial_sql = '''
IF @@rowcount = 0
BEGIN
SELECT 1 FROM [Orders]
END
'''

ds5 = TableauDatasourceGenerator(u'sqlserver', u'My DS', u'demo-dbs', u'Superstore Star Schema', logger,
                                 initial_sql=initial_sql)
custom_sql = '''
SELECT [OrderFact].[OrderID] AS [OrderID],
  [OrderFact].[IDProduct] AS [IDProduct],
  [OrderFact].[IDShipMode] AS [IDShipMode],
  [OrderFact].[IDCustomer] AS [IDCustomer],
  [OrderFact].[IDOrderPriority] AS [IDOrderPriority],
  [OrderFact].[OrderDate] AS [OrderDate],
  [OrderFact].[ShipDate] AS [ShipDate],
  [OrderFact].[OrderQuantity] AS [OrderQuantity],
  [OrderFact].[Sales] AS [Sales],
  [OrderFact].[Discount] AS [Discount],
  [OrderFact].[Profit] AS [Profit],
  [OrderFact].[UnitPrice] AS [UnitPrice],
  [OrderFact].[ShippingCost] AS [ShippingCost],
  [OrderFact].[ProductBaseMargin] AS [ProductBaseMargin],
  [DimCustomer].[IDCustomer] AS [IDCustomer (DimCustomer)],
  [DimCustomer].[CustomerName] AS [CustomerName],
  [DimCustomer].[State] AS [State],
  [DimCustomer].[ZipCode] AS [ZipCode],
  [DimCustomer].[Region] AS [Region],
  [DimCustomer].[CustomerSegment] AS [CustomerSegment]
FROM [dbo].[OrderFact] [OrderFact]
  INNER JOIN [dbo].[DimCustomer] [DimCustomer] ON ([OrderFact].[IDCustomer] = [DimCustomer].[IDCustomer])
'''
ds5.add_first_custom_sql(custom_sql, u'Custom SQL One')

ds5.save_file(u"custom_sql", file_dir)
