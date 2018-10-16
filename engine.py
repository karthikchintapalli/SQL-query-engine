import sys
import re
# q = "select a1, a2,a3 from foo where id in (select id from bar);"

relations = {}
join_conditions = []

with open('./metadata.txt', 'r') as f:
	line = f.readline().strip()
	while line:
		if line == "<begin_table>":
			table_name = f.readline().strip()
			relations[table_name] = {}
			relations[table_name]['schema'] = []
			attr = f.readline().strip()
			while attr != "<end_table>":
				relations[table_name]['schema'].append(attr)
				attr = f.readline().strip()
		line = f.readline().strip()

for table_name in relations:
	relations[table_name]['table'] = []
	relations[table_name]['name'] = table_name
	with open ('./' + table_name + '.csv', 'r') as f:
		for line in f:
			relations[table_name]['table'].append([int(field.strip('"')) for field in line.strip().split(',')])

def table_print(table):
	print ','.join(table['schema'])
	for row in table['table']:
		print ','.join([str(x) for x in row])

	join_conditions[:] = []

def cross_product(table1, table2):
	result_table = {}
	result_table['schema'] = []
	result_table['table'] = []
	result_table['schema'] += [table1['name'] + '.' + field if len(field.split('.')) == 1 else field for field in table1['schema']]
	result_table['schema'] += [table2['name'] + '.' + field if len(field.split('.')) == 1 else field for field in table2['schema']]

	for row1 in table1['table']:
		for row2 in table2['table']:
			result_table['table'].append(row1 + row2)

	return result_table

def select(tables, condition_str):
	if len(tables) > 1:
		joined_table = cross_product(relations[tables[0]], relations[tables[1]])
	else:
		joined_table = cross_product(relations[tables[0]], {'schema': [], 'table': [[]]})

	for i in xrange(2, len(tables)):
		joined_table = cross_product(joined_table, relations[tables[i]])

	result_table = {}
	result_table['schema'] = [x for x in joined_table['schema']]
	result_table['table'] = []

	condition_str = re.sub('(?<=[\w ])(=)(?=[\w ])', '==', condition_str)

	conditions = condition_str.replace(" and ", ",").replace(" or ", ",").replace('(', '').replace(')', '').split(',')

	for condition in conditions:
		if bool(re.match('.*==.*[a-zA-Z]+.*', condition.strip())):
			join_conditions.append((condition.strip().split('==')[0].strip(), condition.strip().split('==')[1].strip()))

	for field in joined_table['schema']:
			condition_str = condition_str.replace(field, 'row[' + str(joined_table['schema'].index(field)) + ']')

	for row in joined_table['table']:
		if eval(condition_str):
			result_table['table'].append(row)

	return result_table

def project(table, fields, distinct, aggregate):

	result_table = {}
	result_table['schema'] = []
	result_table['table'] = []
	if aggregate is None:
		if fields[0] == '*':
			fields = [x for x in table['schema']]

			for field_pair in join_conditions:
				fields = [x for x in fields if x != field_pair[1]]

		result_table['schema'].extend(fields)
		field_indices = []

		for field in fields:
			field_indices.append(table['schema'].index(field))

		for row in table['table']:
			result_row = [row[i] for i in field_indices]
			result_table['table'].append(result_row)

		if distinct:
			temp = sorted(result_table['table'])
			result_table['table'] = [temp[i] for i in range(len(temp)) if i == 0 or temp[i] != temp[i-1]]

	else:
		result_table['schema'].append(aggregate + "(" + fields[0] + ")")
		field_index = table['schema'].index(fields[0])

		temp = [row[field_index] for row in table['table']]

		if aggregate == "sum":
			result_table['table'].append([sum(temp)])

		elif aggregate == "avg":
			result_table['table'].append([(sum(temp) * 1.0)/len(temp)])

		elif aggregate == "max":
			result_table['table'].append([max(temp)])

		elif aggregate == "min":
			result_table['table'].append([min(temp)])

	return result_table

def parse(query):
	if query[-1] != ';':
		print "';' expected"
		return

	query = query.strip(';')
	if bool(re.match('^select.*from.*', query)) is False:
		print "Invalid query"
		return

	distinct = False
	aggregate = None
	select_all = False

	fields = query.split('from')[0].strip('select').strip()
	if bool(re.match('^distinct.*', fields)):
		distinct = True
		fields = fields.replace('distinct', '').strip()

	if bool(re.match('^(sum|max|min|avg)\(.*\)', fields)):
		aggregate = fields.split('(')[0].strip()
		fields = fields.replace(aggregate, '').strip().strip('()')

	fields = fields.split(',')
	fields = [field.strip() for field in fields]

	if len(fields) == 1 and fields[0] == '*':
		select_all = True

	if aggregate is not None and len(fields) > 1:
		print "Too many arguments for aggregate function"
		return

	tables = query.split('from')[1].split('where')[0].strip().split(',')
	tables = [table.strip() for table in tables]

	for table in tables:
		if table not in relations:
			print "Table " + table + " doesn't exist"
			return

	if bool(re.match('^select.*from.*where.*', query)):
		if select_all is False:
			for field in fields:
				field_flag = 0
				for table in tables:
					if field.split('.')[-1] in relations[table]['schema']:
						if len(field.split('.')) == 2:
							if field.split('.')[0] == table:
								field_flag += 1
						else:
							field_flag += 1

				if field_flag != 1:
					print "Invalid field"
					return

		condition_str = query.split('where')[1].strip()
		condition_fields = re.findall(r"[a-zA-Z][\w\.]*", condition_str.replace(' and ', ' ').replace(' or ', ' '))

		condition_fields = list(set(condition_fields))

		for field in condition_fields:
			field_flag = 0
			for table in tables:
				if field.split('.')[-1] in relations[table]['schema']:
					if len(field.split('.')) == 2:
						if field.split('.')[0] == table:
							field_flag += 1
					else:
						field_flag += 1

			if field_flag != 1:
				print "Invalid field"
				return

		if select_all is False:
			for i in xrange(len(fields)):
				if len(fields[i].split('.')) == 1:
					for table in tables:
						if fields[i] in relations[table]['schema']:
							fields[i] = table + '.' + fields[i]
							break

		for field in condition_fields:
			if len(field.split('.')) == 1:
				for table in tables:
					if field in relations[table]['schema']:
						condition_str = re.sub('(?<=[^a-zA-Z0-9])(' + field + ')(?=[\(\)= ])', table + '.' + field, ' ' + condition_str).strip(' ')

		table_print(project(select(tables, condition_str), fields, distinct, aggregate))

	else:
		if len(tables) > 1:
			print "Too many tables"
			return

		if select_all is False:
			for field in fields:
				if field not in relations[tables[0]]['schema']:
					print "Field " + field + " doesn't exist"
					return

		table_print(project(relations[tables[0]], fields, distinct, aggregate)) 

query = sys.argv[1]
query = query.strip('"').strip()
query = re.sub("select ", "select ", query, flags=re.I)
query = re.sub("distinct ", "distinct ", query, flags=re.I)
query = re.sub("from ", "from ", query, flags=re.I)
query = re.sub("where ", "where ", query, flags=re.I)
query = re.sub("AND ", "and ", query)
query = re.sub("OR ", "or ", query)
query = re.sub("MIN", "min", query)
query = re.sub("MAX", "max", query)
query = re.sub("AVG", "avg", query)
query = re.sub("SUM", "sum", query)

parse(query)