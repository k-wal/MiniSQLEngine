import re
import sys 

query = sys.argv[1]

def main_parser(query):
	# as SQL is case insensitive
	query = query
	
	distinct = False
	condition = False
	query_fields = []
	query_tables = []
	query_conditions = []
	or_cond = False
	and_cond = False

	# throw error if no select query found
	if re.split(' ',query.lower())[0] != 'select':
		print("ERROR : no 'select' statement present")
		sys.exit()

	# removing 'select from query'
	query = [i for i in re.split('(?i)select',query) if i]
	query = query[0]

	query = re.split('(?i)from',query)
	# checking if 'from' occurs only once properly
	if len(query) != 2:
		print("ERROR : 'from' not used properly")
		sys.exit()
 	
 	# fields asked in query are 
	query_fields,distinct = find_query_fields(query[0])
	
	# query_fields now has fields as elements
	# query = query without select, from and fields
	query = query[1]

	query = re.split('(?i)where',query)
	condition = False

	# if no 'where' then no condition
	if len(query) == 1:
		condition = False
		query_tables = query[0]

	elif len(query) > 2:
		print("ERROR : more than one 'where' statements found")
		sys.exit()

	# making sure a condition follows 'where'
	else:
		query = [i for i in query if i]
		if len(query) != 2:
			print("ERROR : no condition found after 'where'")
			sys.exit()
		condition = True
		query_tables = query[0]
		query_conditions = query[1]

	query_tables = find_query_tables(query_tables)
	
	if not condition:
		return query_tables,query_fields,distinct,query_conditions

	if len(re.findall('(?i)and|or',query_conditions)) > 1:
		print("ERROR : only one and/or allowed")
		sys.exit()


	condition_and = re.split('(?i)and',query_conditions)
	condition_or = re.split('(?i)or',query_conditions)	

	# check if condition is AND
	if len(condition_and) == 2:
		condition_and = [i for i in condition_and if i]
		if len(condition_and) < 2:
			print("ERROR : condition not specified for 'and'")
			sys.exit()
			return
		query_conditions = condition_and
		query_conditions.append('and')

	# check if condition is OR
	elif len(condition_or) == 2:
		condition_or = [i for i in condition_or if i]
		if len(condition_or) < 2:
			print("ERROR : condition not specified for 'or'")
			sys.exit()
			return
		query_conditions = condition_or
		query_conditions.append('or')

	else:
		query_conditions = [query_conditions, None]

	query_conditions = condition_parser(query_conditions)

	return query_tables,query_fields,distinct,query_conditions

	print(distinct)
	print(query_conditions)
	print(query_tables)
	print(query_fields)


# return array of query fields
def find_query_fields(query_fields):
	query_fields = re.split(',',query_fields)
	distinct = False

	# checking for distinct
	first_field = query_fields[0].lower()
	first_field = [i for i in re.split(' ',first_field) if i]
	if len(first_field) == 0:
		print("ERROR : spare ',' before first field")
		sys.exit()
	
	if first_field[0] == 'distinct':
		if len(first_field) != 2:
			print("ERROR : fields not specified properly after 'distinct'")
			sys.exit()
		distinct = True
		query_fields[0] = [i for i in re.split('(?i)distinct',first_field[1]) if i][0]


	# making sure that commas are used properly for fields
	for i,field in enumerate(query_fields):
		field = [i for i in re.split(' ',field) if i]
		if len(field) != 1:
			print("ERROR : fields not given properly")
			sys.exit()
		query_fields[i] = field[0]
	
	return query_fields,distinct
	

def find_query_tables(query_tables):
	query_tables = re.split(',',query_tables)

	# making sure that commas are used properly for tables
	for i,table in enumerate(query_tables):
		table = [i for i in re.split(' ',table) if i]
		if len(table) != 1:
			print("ERROR : table names not given properly")
			sys.exit()
		query_tables[i] = table[0]

	return query_tables
	
# return 2 conditions (2 fields + operation), AND/OR
def condition_parser(query_conditions):
	
	for c,condition in enumerate(query_conditions[:-1]):
		# make sure there's only one operation
		if len(re.split('<=|>=|=|<|>',condition)) > 2:
			print("ERROR : more than one operator given")
			sys.exit()
		
		if len(re.split('<|=|>',condition)) < 2:
			print(condition)
			print("ERROR : not enough operators given")
			sys.exit()
		
		# find 
		operation = ""
		if len(re.split('<=',condition)) == 2:
			operation = 'less_than_equal'
			condition = re.split('<=',condition)
		elif len(re.split('>=',condition)) == 2:
			operation = 'greater_than_equal'
			condition = re.split('>=',condition)
		elif len(re.split('=',condition)) == 2:
			operation = 'equal'
			condition = re.split('=',condition)
		elif len(re.split('<',condition)) == 2:
			operation = 'less_than'
			condition = re.split('<',condition)
		elif len(re.split('>',condition)) == 2:
			operation = 'greater_than'
			condition = re.split('>',condition)
		
		condition = [i for i in condition if i]
		if len(condition) < 2:
			print("ERROR : only one field specified for condition")
			sys.exit()

		for f,field in enumerate(condition):
			field = [i for i in re.split(' ',field) if i]
			if len(field) != 1:
				print("ERROR : fields not given properly for condition")
				sys.exit()
			condition[f] = field[0]

		query_conditions[c] = [operation,condition[0],condition[1]]
	return query_conditions