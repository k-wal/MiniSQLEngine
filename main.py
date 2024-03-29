import re
import sys 
import parser
import table_func
import csv
import select_func

if len(sys.argv) != 2:
	print("ERROR : number of arguments should be 1")
	sys.exit()

query = sys.argv[1]


query_tables,query_fields,query_distinct,query_conditions = parser.main_parser(query)
table_dict = table_func.get_table_attributes()

# where each field is located : table_name and index
query_fields,located_fields,table_dict = table_func.locate_query_fields(query_fields,query_tables,query_conditions,table_dict)

# loading all tables
tables_data = {}
for table_name in query_tables:
	file_name = 'files/' + table_name + '.csv'
	f = open(file_name,"r")
	data = csv.reader(f)
	table = []
	for row in data:
		# removing all quotes
		for i,col in enumerate(row):
			col = col.replace('"','')
			col = col.replace("'","")
			row[i] = int(col)
		table.append(row)

	tables_data[table_name] = table

# add new table for integer values
for table_name,attributes in table_dict.items():
	if table_name == 'xxx':
		for field in attributes:
			field = int(field)
			if table_name not in tables_data.keys():
				tables_data[table_name] = [[field]]
			else:
				tables_data[table_name][0].append(field)
		if 'xxx' not in query_tables:
			query_tables.append('xxx')

# has an array of columns required of all tables, along with indices of occurence
query_table_fields = {}
aggregate_fields = {}

for field,info in located_fields.items():
	table_name = info['table_name']
	index = info['index']
	if table_name not in query_table_fields.keys():
		query_table_fields[table_name] = [{'column_name':field,'index':index}]

	else:
		query_table_fields[table_name].append({'column_name':field,'index':index})
	
	if 'agg_func' not in info.keys():
		continue
	column_name = info['table_name'] + '.' + info['field']
	
	#if column_name not in query_fields:
	#	query_fields.append(column_name)
	if table_name not in query_table_fields.keys():
		query_table_fields[table_name] = [{'column_name':column_name,'index':info['field_index']}]
	else:
		query_table_fields[table_name].append({'column_name':column_name,'index':info['field_index']})

	agg_field = info['field']
	func = info['agg_func']
	aggregate_fields[field] = info
	agg_value = select_func.get_aggregate(agg_field,table_name,func,query_tables,table_dict,tables_data)
	for i,row in enumerate(tables_data[table_name]):
		tables_data[table_name][i].append(agg_value)


for i,condition in enumerate(query_conditions[:-1]):
	for j,field in enumerate(condition[1:3]):
		
		if len(re.split('\.',field)) == 2:
			table_name = re.split('\.',field)[0]
			if table_name not in query_tables:
				print("ERROR : table ",table_name," not in query")
				sys.exit()
			continue
		is_present = False
		for table in query_tables:
			if field in table_dict[table]:
				if is_present == True:
					print("ERROR : field found in multiple tables")
					sys.exit()
				query_conditions[i][j+1] = table + '.' + field
				is_present = True
		if not is_present:
			print("ERROR : field not found in tables")
			sys.exit()

	
joining_fields = select_func.get_joining_fields(query_conditions)


big_cols,big_table = select_func.create_joined_table(query_tables,query_table_fields,tables_data)
#select_func.display_result(big_cols,big_table)

filtered_table = select_func.apply_conditions(big_cols,big_table,query_conditions,query_distinct)
#select_func.display_result(big_cols,filtered_table)

big_cols,filtered_table = select_func.cal_aggregate(big_cols,filtered_table,aggregate_fields)
#select_func.display_result(big_cols,filtered_table)

selected_table,query_fields = select_func.select_to_display(query_fields,big_cols,filtered_table,aggregate_fields)

selected_table,query_fields = select_func.remove_joining_fields(query_fields,selected_table,joining_fields)

select_func.display_result(query_fields,selected_table)
