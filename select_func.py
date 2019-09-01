import itertools


def create_joined_table(query_tables,query_table_fields,tables_data):

	big_table = []
	big_cols = []

	# return a join table
	for table in query_tables:
		indices = []
		selected = []
		
		if table not in query_table_fields.keys():
			continue
		
		# append to list of column names for big table (big_cols) and list of indices for current table to be selected
		for field_info in query_table_fields[table]:
			big_cols.append(field_info['column_name'])
			indices.append(field_info['index'])

		# for each row in table, append the required indices
		for row in tables_data[table]:
			add_row = []
			for i,field in enumerate(row):
				if i in indices:
					add_row.append(field)
			
			selected.append(add_row)
			
		if big_table == []:
			big_table = selected
			continue
			
		temp = []
		# take cartesian product with previous product
		for e1,e2 in itertools.product(big_table,selected):
			temp.append(e1+e2)
		
		big_table = temp
	return big_cols,big_table

# select based on conditions
def apply_conditions(cols,table,query_conditions):
	if query_conditions == []:
		return table
	conditions = []
	if len(query_conditions) == 2:
		conditions = [query_conditions[0]]
	else:
		conditions = query_conditions[0:2]
	
	for cond in conditions:
		op1,op2 = cond[1],cond[2]
		operation = cond[0]

		for i,col in enumerate(cols):
			if col==op1:
				ind1 = i
			elif col==op2:
				ind2 = i
		
		for index,row in enumerate(table):
			if operation == 'equal' and row[ind1]==row[ind2]:
				table[index].append(True)
			elif operation == 'less_than' and row[ind1]<row[ind2]:
				table[index].append(True)
			elif operation == 'greater_than' and row[ind1]>row[ind2]:
				table[index].append(True)
			elif operation == 'less_than_equal' and row[ind1]<=row[ind2]:
				table[index].append(True)
			elif operation == 'greater_than_equal' and row[ind1]>=row[ind2]:
				table[index].append(True)
			else:
				table[index].append(False)

	if len(query_conditions) == 2:
		results = [row[:-1] for row in table if row[-1:][0]]

	else:
		if query_conditions[2]=='and':
			results = [row[:-2] for row in table if row[-1:][0] and row[-2:-1][0]]
		else:
			results = [row[:-2] for row in table if row[-1:][0] or row[-2:-1][0]]
			

	return results


def display_result(cols,results):
	for col in cols:
		print(col,end = ' \t\t')
	print("\n")
	print("-"*15*len(cols))

	for row in results:
		for col in row:
			print(col,end = ' \t\t')
		print("\n")
