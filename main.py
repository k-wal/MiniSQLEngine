import re
import sys 
import parser

def get_table_attributes():
	meta_file = open('files/metadata.txt',"r")
	line = meta_file.readline().rstrip()
	cur_table = ""
	table_dict = {}

	while line != "":
		# go through each table
		if line == '<begin_table>':
			line = meta_file.readline().rstrip()
			cur_table = line
			table_dict[cur_table] = []
			line = meta_file.readline().rstrip()
		
			while line != '<end_table>':
				table_dict[cur_table].append(line)
				line = meta_file.readline().rstrip()
	
		line = meta_file.readline().rstrip()
			
	return table_dict




def locate_query_fields(query_fields,query_tables,query_conditions,table_dict):

	query_fields_table = {}

	# if * is present, make sure its the only field specified
	if '*' in query_fields:
		if len(query_fields) >1:
			print("ERROR : *,_ is not allowed")
			sys.exit()
		query_fields = []
		for table in query_tables:
			if table not in table_dict.keys():
				print("ERROR : table ",table," not in database")
				sys.exit()		
			attributes = table_dict[table]
			for index,attribute in enumerate(attributes):
				field_name = table + '.' + attribute
				query_fields_table[field_name] = {'table_name':field_name , 'index':index}
		return query_fields_table
	

	for condition in query_conditions[:-1]:
		query_fields += condition[1:3]


	# splitting if table name is attached to column name
	for field in query_fields:
		field_name = field
		field = re.split('\.',field)
		if len(field) > 2:
			print("ERROR : '.' not used properly in fields")
			sys.exit()
		
		# if no table is specified
		if len(field) == 1:
			field = field[0]
			for table in query_tables:
				if table not in table_dict.keys():
					print("ERROR : table "+table+" not in database")
					sys.exit()
				attributes = table_dict[table]
				for index,attribute in enumerate(attributes):
					if attribute == field:
						if field in query_fields_table.keys():
							old = query_fields_table[field]
							if old['table_name'] != table:
								print("ERROR : columns with same name ",field," present")
								sys.exit()
						query_fields_table[field] = {'table_name' : table,'index' : index}

		# if table is specified
		elif len(field) == 2:
			full_field = field[0]+'.'+field[1]
			table_name = field[0]
			field = field[1]
			# if table doesn't exist
			if table_name not in table_dict.keys():
				print("ERROR : no table ",table_name," present")
				sys.exit()
			# if table name isn't given in the query after FROM
			if table_name not in query_tables:
				print("ERROR : no table ",table_name," given in query")
				sys.exit()
			
			attributes = table_dict[table_name]
			# go through the table, find the attribute
			for index,attribute in enumerate(attributes):
				if attribute == field:
					# if attribute asked for twice in the query, 
					if full_field not in query_fields_table.keys():
						query_fields_table[full_field] = {'table_name' : table_name, 'index' : index}
		
		if field_name not in query_fields_table.keys():
			print("ERROR : column ",field_name," doesn't exist")
			sys.exit()
	return query_fields_table



query = sys.argv[1]

print(parser.main_parser(query))
query_tables,query_fields,query_distinct,query_conditions = parser.main_parser(query)
table_dict = get_table_attributes()
print(table_dict)
located_fields = locate_query_fields(query_fields,query_tables,query_conditions,table_dict)
print(located_fields)
