import itertools
import re
import copy

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
			for i in indices:
				add_row.append(row[i])
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
def apply_conditions(cols,table,query_conditions,distinct):
	if query_conditions == []:
		return get_distinct(table,distinct)
	conditions = []
	# if 1 condition : add 1, if 2:add both
	if len(query_conditions) == 2:
		conditions = [query_conditions[0]]
	else:
		conditions = query_conditions[0:2]
	# going through each condition
	for cond in conditions:
		op1,op2 = cond[1],cond[2]
		operation = cond[0]
		# finding indices of operands involved
		for i,col in enumerate(cols):
			if col==op1:
				ind1 = i
			elif col==op2:
				ind2 = i
		# checking each row, marking true/false for each condition
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

	# return rows in result that are marked true 
	if len(query_conditions) == 2:
		results = [row[:-1] for row in table if row[-1:][0]]
	# return according to and/or in conditions
	else:
		if query_conditions[2]=='and':
			results = [row[:-2] for row in table if row[-1:][0] and row[-2:-1][0]]
		else:
			results = [row[:-2] for row in table if row[-1:][0] or row[-2:-1][0]]
	return get_distinct(results,distinct)	

# if distinct = True, return table with distinct rows
def get_distinct(table,distinct):
	if not distinct:
		return table
	
	final = []
	for row in table:
		if row not in final:
			final.append(row)

	return final

# display results in a pretty way
def display_result(cols,results):
	for col in cols:
		print(col,end = ' \t\t')
	print("\n")
	print("-"*15*len(cols))

	for row in results:
		for col in row:
			print(col,end = ' \t\t')
		print("\n")

# select columns to display finally
def select_to_display(query_fields,big_cols,table,agg_fields):
	indices = []

	cols = []

	for field in query_fields:
		for i,col in enumerate(big_cols):
			if field == col:
				indices.append(i)

	result = []
	for row in table:
		r = []
		for index in indices:
			r.append(row[index])
		result.append(r)

	# if first field is aggregate, print only that field
	if len(agg_fields)==0:
		return result,query_fields
	
	return [[result[0][0]]],[query_fields[0]]
	

# return aggregate
def get_aggregate(field,table,func,query_tables,table_dict,tables_data):
	# get index of attribute to aggregate
	for index,attribute in enumerate(table_dict[table]):
		if field == attribute:
			break
	sum_value = 0
	max_value = tables_data[table][0][index]
	min_value = tables_data[table][0][index]
	length = len(tables_data[table])

	# go through each row, calculate all aggregates for attribute with index 'index'
	for row in tables_data[table]:
		val = row[index]
		if val<min_value:
			min_value = val
		if val>max_value:
			max_value = val
		sum_value += val

	if func == 'sum':
		return sum_value
	if func == 'max':
		return max_value
	if func == 'min':
		return min_value
	if func == 'average' or func == 'avg':
		return sum_value/length

# calculating aggregate after where condition
def cal_aggregate(fields,table,agg_fields):
	if len(agg_fields) == 0:
		return fields,table

	for agg_field,info in agg_fields.items():
		column_name = info['table_name'] + '.' + info['field']
		for index,field in enumerate(fields):
			if column_name == field:
				break
		func = info['agg_func']
		
		sum_value = 0
		min_value = table[0][index]
		max_value = table[0][index]
		for row in table:
			val = row[index]
			if val<min_value:
				min_value = val
			if val>max_value:
				max_value = val
			sum_value += val
		if func == 'max':
			agg_value = max_value	
		if func == 'min':
			agg_value = min_value	
		if func == 'sum':
			agg_value = sum_value	
		if func == 'avg':
			agg_value = sum_value / len(table)
		
		for field_index,field in enumerate(fields):
			if field != agg_field:
				continue
			for i,row in enumerate(table):
				table[i][field_index] = agg_value
	return fields,table

def get_joining_fields(query_conditions):
	if len(query_conditions) == 0:
		return []
	
	result = []

	# if 1 condition : add 1, if 2:add both
	if len(query_conditions) == 2:
		conditions = [query_conditions[0]]
	else:
		conditions = query_conditions[0:2]
	# going through each condition
	for cond in conditions:
		op1,op2 = cond[1],cond[2]
		operation = cond[0]
		if operation != 'equal':
			continue
		table1 = re.split('\.',op1)[0]
		table2 = re.split('\.',op2)[0]
		
		# if belong to same table, they're not joining
		if table1 == table2:
			continue

		result.append([op1,op2])

	return result

def remove_joining_fields(fields,table,joining_fields):
	for joining_pair in joining_fields:
		result = []
		field1 = joining_pair[0]
		field2 = joining_pair[1]
		occur1 = False
		occur2 = False

		indices = []
		for index,field in enumerate(fields):
			if field == field1:
				if occur2:
					continue
				occur1 = True
			if field == field2:
				if occur1:
					continue
				occur2 = True
			indices.append(index)

		for row in table:
			r = []
			for i in indices:
				r.append(row[i])
			result.append(r)
		f = []
		for i in indices:
			f.append(fields[i])

		fields = copy.deepcopy(f)
		table = copy.deepcopy(result)
	return table,fields