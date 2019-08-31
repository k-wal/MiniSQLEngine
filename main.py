import re
import sys 
import parser

query = sys.argv[1]

print(parser.main_parser(query))
query_tables,query_fields,query_distinct,query_conditions = parser.main_parser(query)

