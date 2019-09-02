# Mini SQL Engine

A SQL engine to query existing tables.

###Features

+ `SELECT` queries : basic select queries, including of the type `select * from table`
+ `WHERE` conditions : only comparisons like <,>,<=,>=,= allowed(within fields and integers), maximum 1 `and`/`or` allowed, maximum 2 conditions in one query allowed.
+ `DISTINCT` : can choose to display only distinct rows
+ Aggregates : `max`, `min`, `average`, `sum` of one column

<br/>

###Additional Features

+ Error Handling : for scenarios where tables, fields given in query don't exist or where there's a mistake in syntax
+ Case Sensitivity : as in the case of MySQL, keywords like `select`, `distinct`, `from` and `where` are case insensitive and column and table names are case sensitive. 
+ No external parser used


<br/>

### Table Data

All table data is in a folder called 'files' in the same directory as the python files

+ **metadata.txt** : contains schema of all tables
+ **table_name.csv** : contains data for a table called *table\_name*

<br/>

### Python Files
+ **main.py** : Main file, calls all other functions
+ **parser.py** : To parse the query
+ **table\_func.py** : contains functions to consolidate tables and correspoding field information
+ **select\_func.py** : contains functions to create tables for the query

### Libraries Required
Only default python libraries are used

+ re
+ sys
+ csv
+ itertools

### How to run
Let query be : Q

`python3 main.py "Q"`
