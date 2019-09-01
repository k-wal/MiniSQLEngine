import re
import sys


# return a dictionary with table_names as keys, and for each table name return set of attributes
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


def is_int(s):
	try:
		int(s)
		return True
	except:
		return False


def is_aggregate(s,table_dict):
	func = ""
	field = ""
	table = ""
	if len(re.findall('max\(.*\)',s)) == 1:
		func = 'max'
		field = s[4:-1]
	elif len(re.findall('min\(.*\)',s)) == 1:
		func = 'min'
		field = s[4:-1]
	elif len(re.findall('sum\(.*\)',s)) == 1:
		func = 'sum'
		field = s[4:-1]
	elif len(re.findall('average\(.*\)',s)) == 1:
		func = 'avg'
		field = s[8:-1]
	elif len(re.findall('avg\(.*\)',s)) == 1:
		func = 'avg'
		field = s[4:-1]
	

	if field == "":
		return False,"","",""	
	
	if func == "":
		print("ERROR : aggregate function not recognized")
		sys.exit()
	if re.split('.',field) == 2:
		table = re.split('.',field)[0]
		for index,attribute in enumerate(table_dict[table]):
			if attribute == re.split('.',field)[1]:
				return func,field,table,index

	for table_name,attributes in table_dict.items():
		for index,attribute in enumerate(attributes):
			if attribute == field:
				return func,field,table_name,index

	print("ERROR : field ",field," not recognized")
	sys.exit()


# return a dicitonary with fields in query as keys, and their table_name and occurence index in table correspondingly
def locate_query_fields(query_fields,query_tables,query_conditions,table_dict):

	query_fields_table = {}
	# return_fields : new set of fields to display
	return_fields = []
	
	# if * is present, make sure its the only field specified
	if '*' in query_fields:
		if len(query_fields) >1:
			print("ERROR : *,_ is not allowed")
			sys.exit()
		query_fields_original = []
		for table in query_tables:
			if table not in table_dict.keys():
				print("ERROR : table ",table," not in database")
				sys.exit()		
			attributes = table_dict[table]
			for index,attribute in enumerate(attributes):
				field_name = table + '.' + attribute
				query_fields_original.append(field_name)
				query_fields_table[field_name] = {'table_name':table , 'index':index, 'column_name':field_name}
				return_fields.append(field_name)

	
	# count of fields to display : to make sure fields in condition aren't added to return_fields
	count_query_fields_original = len(query_fields)
	count_cond_fields = 0

	for condition in query_conditions[:-1]:
		query_fields += condition[1:3]
		count_cond_fields += 2


	# initial  count of fields, so that new fields aren't added because the already new ones
	initial_count_fields = len(query_fields)
	
	# splitting if table name is attached to column name
	for field_index,field in enumerate(query_fields):
		if field == '*':
			continue
		if field_index == initial_count_fields:
			break
		is_found = False

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
						full_field = table + '.' + field
						if not is_found:
							is_found =  True
							query_fields[field_index] = full_field
							if field_index <= count_query_fields_original and full_field not in return_fields:
								return_fields.append(full_field)
								
						else:
							query_fields.append(full_field)
							if field_index <= count_query_fields_original and full_field not in return_fields:
									return_fields.append(full_field)
						
						# making sure that current field occurs only once
						for qfield,qdict in query_fields_table.items():
							if qdict['table_name'] != table and qdict['column_name'] == field:
								print("ERROR : column ",field," present in multiple tables.")		
								sys.exit()

						if full_field not in query_fields_table.keys():
							query_fields_table[full_field] = {'table_name' : table,'index' : index,'column_name':field}
					
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
					is_found = True
					
					if field_index <= count_query_fields_original and full_field not in return_fields:
						return_fields.append(full_field)
						
					# if attribute asked for twice in the query, 
					if full_field not in query_fields_table.keys():
						query_fields_table[full_field] = {'table_name' : table_name, 'index' : index, 'column_name' : field}
		
		if not is_found:

			func,field_agg,table_agg,index_agg = is_aggregate(field_name,table_dict)
			if func is not False:
				l = len(table_dict[table_agg])
				table_dict[table_agg].append(field)
				full_field = func + '(' + table_agg+'.'+field_agg+')'
				query_fields_table[full_field] = {'table_name':table_agg,'index':l,'column_name':full_field,'agg_func':func,'field':field_agg} 
				return_fields.append(full_field)

			elif not is_int(field_name):
				print("ERROR : column ",field_name," doesn't exist")
				sys.exit()
			# if integer value, add to a new table full of constants
			else:
				new_table_name = 'xxx'
				full_field = new_table_name + '.' + field
				if new_table_name not in table_dict.keys():
					table_dict[new_table_name] = [field_name]
				else:
					table_dict[new_table_name].append(field_name)
				i = len(table_dict['xxx'])-1
				query_fields_table[full_field]  = {'table_name':new_table_name,'index' : i, 'column_name' : full_field,'field':field_agg}
	return return_fields,query_fields_table,table_dict
