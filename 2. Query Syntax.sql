CREATE DATABASE IF NOT EXISTS SNOWFLAKE_TUTORIALS;
USE SNOWFLAKE_TUTORIALS;
CREATE SCHEMA IF NOT EXISTS LEARNING;
USE SCHEMA LEARNING;

-----------------------------------------New Topic:SELECT Statement:-----------------------------------------
/*
-- Syntax
[ ... ]
SELECT 
	[ { ALL | DISTINCT } ]
	[ TOP <n> ]
	[{<object_name>|<alias>}.]*

	[ ILIKE '<pattern>' ]

	[ EXCLUDE
	 {
	   <col_name> | ( <col_name>, <col_name>, ... )
	 }
	]

	[ REPLACE
	 {
	   ( <expr> AS <col_name> [ , <expr> AS <col_name>, ... ] )
	 }
	]

	[ RENAME
	 {
	   <col_name> AS <col_alias>
	   | ( <col_name> AS <col_alias>, <col_name> AS <col_alias>, ... )
	 }
	]
	
-- Parameters
1. ALL | DISTINCT
	* ALL includes all values in the result set. [Default]
	* DISTINCT eliminates duplicate values from the result set.
	
2. TOP <n>
	Specifies the maximum number of results to return.
	
3. <object_name> or <alias>
	Specifies the object identifier or object alias (column name) as defined in the FROM clause.
	
4. ILIKE '<pattern>'
	Specifies that only the columns that match <pattern> should be included in the results.
	In <pattern>, you can use the following SQL wildcards:
		* Use an underscore (_) to match any single character.
		* Use a percent sign (%) to match any sequence of zero or more characters.
		
5. EXCLUDE <col_name> or EXCLUDE (<col_name>, <col_name>, ...)
	Specifies the columns that should be excluded from the results.
		
6. REPLACE (<expr> AS <col_name> [ , <expr> AS <col_name>, ...] )
	
7. RENAME <col_name> AS <col_alias> or RENAME (<col_name> AS <col_alias>, <col_name> AS <col_alias>, ...)
	
Note:
	When specifying a combination of keywords after SELECT *:
	You cannot specify both ILIKE and EXCLUDE.
	If you specify EXCLUDE with RENAME or REPLACE:
		You must specify EXCLUDE before RENAME or REPLACE
		You cannot specify the same column in EXCLUDE and RENAME
	If you specify ILIKE with RENAME or REPLACE, you must specify ILIKE first
	If you specify REPLACE and RENAME:
		You must specify REPLACE first
	You can specify the same column name in REPLACE and RENAME

8. $<col_position>
	Specifies the position of the column (1-based) as defined in the FROM clause.
*/

-- Setting up the data for the examples

CREATE TABLE employee_table (
	employee_ID INTEGER,
	last_name VARCHAR,
	first_name VARCHAR,
	department_ID INTEGER
);

CREATE TABLE department_table (
	department_ID INTEGER,
	department_name VARCHAR
);

INSERT INTO employee_table (employee_ID, last_name, first_name, department_ID) VALUES
	(101, 'Montgomery', 'Pat', 1),
	(102, 'Levine', 'Terry', 2),
	(103, 'Comstock', 'Dana', 2);

INSERT INTO department_table (department_ID, department_name) VALUES
    (1, 'Engineering'),
    (2, 'Customer Support'),
    (3, 'Finance');
	
-- Selecting all columns in the table
SELECT * FROM employee_table;

-- Selecting all columns with names that match a pattern
SELECT * ILIKE '%id%' FROM employee_table;

-- Selecting all columns except one column
SELECT * EXCLUDE department_id FROM employee_table;

-- Selecting all columns except two or more columns
SELECT * EXCLUDE (department_id, employee_id) FROM employee_table;

-- Selecting all columns and renaming one column
SELECT * RENAME department_id AS department FROM employee_table;

-- Selecting all columns and renaming multiple columns
SELECT * RENAME (department_id AS department, employee_id AS id) FROM employee_table;

-- Selecting all columns, excluding a column, and renaming multiple columns¶
SELECT * EXCLUDE first_name RENAME (department_id AS department, employee_id AS id) FROM employee_table;

-- Selecting all columns with names that match a pattern and renaming a column
SELECT * ILIKE '%id%' RENAME department_id AS department FROM employee_table;

-- Selecting all columns and replacing the value of a column
SELECT * REPLACE ('DEPT-' || department_id AS department_id) FROM employee_table;

-- Selecting all columns, replacing the value of a column, and renaming the column
SELECT * REPLACE ('DEPT-' || department_id AS department_id) RENAME department_id AS department FROM employee_table;

-- Selecting all columns with names that match a pattern and replacing the value in a column
SELECT * ILIKE '%id%' REPLACE('DEPT-' || department_id AS department_id) FROM employee_table;

-- Selecting all columns from multiple tables, excluding a column, and renaming a column
SELECT
	employee_table.* EXCLUDE department_id,
	department_table.* RENAME department_name AS department
FROM employee_table INNER JOIN department_table
ON employee_table.department_id = department_table.department_id
ORDER BY department, last_name, first_name;

-- Selecting a single column by name
SELECT last_name FROM employee_table WHERE employee_ID = 101;

-- Selecting multiple columns by name from joined tables
SELECT department_name, last_name, first_name
FROM employee_table INNER JOIN department_table
ON employee_table.department_ID = department_table.department_ID
ORDER BY department_name, last_name, first_name;

-- Selecting a column by position
SELECT $2 FROM employee_table ORDER BY $2;

-- Specifying an alias for a column in the output
SELECT pi() * 2.0 * 2.0 AS area_of_circle;




-----------------------------------------:WITH Statement:-----------------------------------------
/*
-- Syntax
-- 1. Subquery
[ WITH
		<cte_name1> [ ( <cte_column_list> ) ] AS ( SELECT ...  )
	[ , <cte_name2> [ ( <cte_column_list> ) ] AS ( SELECT ...  ) ]
	[ , <cte_nameN> [ ( <cte_column_list> ) ] AS ( SELECT ...  ) ]
]
SELECT ...

-- 2. Recursive CTE
[ WITH [ RECURSIVE ]
		<cte_name1> ( <cte_column_list> ) AS ( anchorClause UNION ALL recursiveClause )
	[ , <cte_name2> ( <cte_column_list> ) AS ( anchorClause UNION ALL recursiveClause ) ]
	[ , <cte_nameN> ( <cte_column_list> ) AS ( anchorClause UNION ALL recursiveClause ) ]
]
SELECT ...
-- Where:
anchorClause ::=
    SELECT <anchor_column_list> FROM ...

recursiveClause ::=
    SELECT <recursive_column_list> FROM ... [ JOIN ... ]
	

# Attention: 
	When using a recursive CTE, it is possible to create a query that goes into an infinite loop and consumes credits until the query succeeds, the query times out (e.g. exceeds the number of seconds specified by the STATEMENT_TIMEOUT_IN_SECONDS parameter), or you cancel the query.
	
2.1 Anchor clause
	The anchor clause in a recursive CTE is a SELECT statement.
	The anchor clause is executed once during the execution of the statement in which it is embedded; it runs before the recursive clause and generates the first set of rows from the recursive CTE. These rows are not only included in the output of the query, but also referenced by the recursive clause.
	
2.2 Recursive clause
	The recursive clause is a SELECT statement. This SELECT is restricted to projections, filters, and joins (inner joins and outer joins in which the recursive reference is on the preserved side of the outer join). The recursive clause cannot contain:
		* Aggregate or window functions,
		* GROUP BY, ORDER BY, LIMIT, or DISTINCT.
	The recursive clause usually includes a JOIN that joins the table that was used in the anchor clause to the CTE. However, the JOIN can join more than one table or table-like data source (view, etc.).
	The first iteration of the recursive clause starts with the data from the anchor clause. That data is then joined to the other table(s) in the FROM clause of the recursive clause.
*/
-- Setting up the data for the examples

CREATE OR REPLACE TABLE employees (title VARCHAR, employee_ID INTEGER, manager_ID INTEGER);

INSERT INTO employees (title, employee_ID, manager_ID) VALUES
	('President', 1, NULL),  -- The President has no manager.
	('Vice President Engineering', 10, 1),
	('Programmer', 100, 10),
	('QA Engineer', 101, 10),
	('Vice President HR', 20, 1),
	('Health Insurance Analyst', 200, 20);

    
-- Non-recursive examples
WITH src as(
	SELECT
		emps.title,
		emps.employee_ID,
		mgrs.employee_ID AS MANAGER_ID, 
		mgrs.title AS "MANAGER TITLE"
	FROM employees AS emps LEFT OUTER JOIN employees AS mgrs
	ON emps.manager_ID = mgrs.employee_ID
	ORDER BY mgrs.employee_ID NULLS FIRST, emps.employee_ID
) SELECT * FROM src;

-- New Topic: RECURSIVE example
WITH RECURSIVE current_f (current_val, previous_val) AS
(
	SELECT 0, 1
	UNION ALL 
	SELECT current_val + previous_val, current_val FROM current_f
	WHERE current_val + previous_val < 100
) SELECT current_val, previous_val FROM current_f ORDER BY current_val;

-- RECURSIVE example with join
WITH RECURSIVE managers (indent, employee_ID, manager_ID, employee_title) AS (
	SELECT '' AS indent, employee_ID, manager_ID, title AS employee_title FROM employees WHERE title = 'President' -- Anchor Clause
	UNION ALL
	SELECT indent || '--- ', employees.employee_ID, employees.manager_ID, employees.title FROM employees JOIN managers ON employees.manager_ID = managers.employee_ID	-- Recursive clause
) SELECT indent || employee_title AS Title, employee_ID, manager_ID FROM managers;




-----------------------------------------New Topic:INTO Statement:-----------------------------------------
/*
Sets Snowflake Scripting variables to the values in a row returned by a SELECT statement.
-- Syntax
SELECT <expression1>
   [ , <expression2> ]
   [ , <expressionN> ]
[ INTO :<variable1> ]
   [ , :<variable2> ]
   [ , :<variableN> ]
FROM ...
WHERE ...
[ ... ]
*/

-- Setting up the data for the examples
CREATE OR REPLACE TABLE some_data (id INTEGER, name VARCHAR);
INSERT INTO some_data (id, name) VALUES
	(1, 'a'),
	(2, 'b');


-- Example
DECLARE
	id INTEGER;
	name VARCHAR;
BEGIN
	SELECT id, name INTO :id, :name FROM some_data WHERE id = 1;
	RETURN id || ' ' || name;
END;



-----------------------------------------:FROM Statement:-----------------------------------------
/*
Specifies the tables, views, or table functions to use in a SELECT statement.
-- Syntax
SELECT ...
FROM objectReference [ JOIN objectReference [ ... ] ]
[ ... ]

where:

objectReference ::=
   {
      [<namespace>.]<object_name>
           [ AT | BEFORE ( <object_state> ) ]
           [ CHANGES ( <change_tracking_type> ) ]
           [ MATCH_RECOGNIZE ]
           [ PIVOT | UNPIVOT ]
           [ [ AS ] <alias_name> ]
           [ SAMPLE ]
     | <table_function>
           [ PIVOT | UNPIVOT ]
           [ [ AS ] <alias_name> ]
           [ SAMPLE ]
     | ( VALUES (...) )
           [ SAMPLE ]
     | [ LATERAL ] ( <subquery> )
           [ [ AS ] <alias_name> ]
     | @[<namespace>.]<stage_name>[/<path>]
           [ ( FILE_FORMAT => <format_name>, PATTERN => '<regex_pattern>' ) ]
           [ [AS] <alias_name> ]
     | DIRECTORY( @<stage_name> )
   }
*/

-- Setting up the data for the examples
CREATE TABLE ftable1 (retail_price FLOAT, wholesale_cost FLOAT, description VARCHAR);
INSERT INTO ftable1 (retail_price, wholesale_cost, description) 
	VALUES (14.00, 6.00, 'bling');
	
-- Here is a basic example of using the FROM clause
SELECT description, retail_price, wholesale_cost FROM ftable1;

-- This example creates an inline view and then uses it in the query
SELECT v.profit FROM (SELECT retail_price - wholesale_cost AS profit FROM ftable1) AS v;

-- This example queries a sample of 10% of the data in the table:
SELECT * FROM sales SAMPLE(10);

-- This example executes a UDTF (user-defined table function):
SELECT * FROM TABLE(Fibonacci_Sequence_UDTF(6.0::FLOAT));

/*
These examples use an AT clause to return historical data from the following specified points in the past:
	* One day earlier than the current time (-86400 = -3600 * 24).
	* Specific time and day.
*/
-- New Topic:
SELECT *
    FROM sales AT(OFFSET => -86400);
SELECT *
    FROM sales AT(TIMESTAMP => '2018-07-27 12:00:00'::TIMESTAMP);
	
-- This example queries files located in a named stage
SELECT
	v.$1, v.$2, ...
FROM
	@my_stage( FILE_FORMAT => 'csv_format', PATTERN => '.*my_pattern.*') v;
	
-- This example retrieves all metadata columns in a directory table for a stage named mystage
SELECT * FROM DIRECTORY(@mystage);

-- This example retrieves the FILE_URL column values from a directory table for files greater than 100 K bytes in size
SELECT FILE_URL FROM DIRECTORY(@mystage) WHERE SIZE > 100000;

-- This example retrieves the FILE_URL column values from a directory table for comma-separated value files
SELECT FILE_URL FROM DIRECTORY(@mystage) WHERE RELATIVE_PATH LIKE '%.csv';



-----------------------------------------New Topic:AT | BEFORE Statement:-----------------------------------------
/*
The AT or BEFORE clause is used for Snowflake Time Travel. In a query, it is specified in the FROM clause immediately after the table name, and it determines the point in the past from which historical data is requested for the object:
	* The AT keyword specifies that the request is inclusive of any changes made by a statement or transaction with a timestamp equal to the specified parameter.
	* The BEFORE keyword specifies that the request refers to a point immediately preceding the specified parameter. This point in time is just before the statement, identified by its query ID, is completed.
	
-- Syntax
SELECT ...
FROM ...
  {
   AT( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) |
   BEFORE( STATEMENT => <id> )
  }
[ ... ]
*/

-- AT Example
-- Setting up the data for the examples
CREATE OR REPLACE TABLE tt1 (c1 INT, c2 INT);
INSERT INTO tt1 VALUES(1,2);
INSERT INTO tt1 VALUES(2,3);

SHOW TERSE TABLES LIKE 'tt1';

INSERT INTO tt1 VALUES(3,4);

SELECT * FROM tt1 at(TIMESTAMP => '2024-06-05 15:29:00'::TIMESTAMP_LTZ);

-- Select historical data from a table as of 5 minutes ago:
SELECT * FROM my_table AT(OFFSET => -60*5) AS T WHERE T.flag = 'valid';

-- BEFORE Example
-- Select historical data from a table up to, but not including any changes made by the specified transaction:
SELECT * FROM my_table BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');

-- Return the difference in table data resulting from the specified transaction
SELECT oldt.* ,newt.* FROM my_table BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') AS oldt
FULL OUTER JOIN my_table AT(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') AS newt
ON oldt.id = newt.id
WHERE oldt.id IS NULL OR newt.id IS NULL;


-----------------------------------------New Topic:CHANGES Statement:-----------------------------------------
/*
The CHANGES clause enables querying the change tracking metadata for a table or view within a specified interval of time without having to create a stream with an explicit transactional offset. Multiple queries can retrieve the change tracking metadata between different transactional start and endpoints.

-- Syntax
SELECT ...
FROM ...
   CHANGES ( INFORMATION => { DEFAULT | APPEND_ONLY } )
   AT ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) | BEFORE ( STATEMENT => <id> )
   [ END( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> } ) ]
[ ... ]
*/

-- Example 
CREATE OR REPLACE TABLE t1 (
   id number(8) NOT NULL,
   c1 varchar(255) default NULL
 );

-- Enable change tracking on the table.
 ALTER TABLE t1 SET CHANGE_TRACKING = TRUE;

 -- Initialize a session variable for the current timestamp.
 SET ts1 = (SELECT CURRENT_TIMESTAMP());

 INSERT INTO t1 (id,c1)
 VALUES
 (1,'red'),
 (2,'blue'),
 (3,'green');

 DELETE FROM t1 WHERE id = 1;

 UPDATE t1 SET c1 = 'purple' WHERE id = 2;

 -- Query the change tracking metadata in the table during the interval from $ts1 to the current time.
 -- Return the full delta of the changes.
 SELECT *
 FROM t1
   CHANGES(INFORMATION => DEFAULT)
   AT(TIMESTAMP => $ts1);

 -- Query the change tracking metadata in the table during the interval from $ts1 to the current time.
 -- Return the append-only changes.
 SELECT *
 FROM t1
   CHANGES(INFORMATION => APPEND_ONLY)
   AT(TIMESTAMP => $ts1);
   
 
 
 -----------------------------------------New Topic:CONNECT BY Statement:-----------------------------------------
 /*
Joins a table to itself to process hierarchical data in the table. The CONNECT BY subclause of the FROM clause iterates to process the data.

-- Syntax
SELECT <column_list> [ , <level_expression> ]
  FROM <data_source>
    START WITH <predicate>
    CONNECT BY [ PRIOR ] <col1_identifier> = [ PRIOR ] <col2_identifier>
           [ , [ PRIOR ] <col3_identifier> = [ PRIOR ] <col4_identifier> ]
           ...
  ...
 */
-- Setting up the data for the examples
CREATE OR REPLACE TABLE employees (title VARCHAR, employee_ID INTEGER, manager_ID INTEGER);
INSERT INTO employees (title, employee_ID, manager_ID) VALUES
	('President', 1, NULL),  -- The President has no manager.
	('Vice President Engineering', 10, 1),
	('Programmer', 100, 10),
	('QA Engineer', 101, 10),
	('Vice President HR', 20, 1),
	('Health Insurance Analyst', 200, 20);
	
-- Example 1
SELECT employee_ID, manager_ID, title
FROM employees
START WITH title = 'President'
CONNECT BY
	manager_ID = PRIOR employee_id
ORDER BY employee_ID;
  
 -- Example 2 using SYS_CONNECT_BY_PATH to show the hierarchy from the President down to the current employee
SELECT SYS_CONNECT_BY_PATH(title, ' -> '), employee_ID, manager_ID, title
FROM employees
START WITH title = 'President'
CONNECT BY
	manager_ID = PRIOR employee_id
ORDER BY employee_ID;



-----------------------------------------:JOIN Statement:-----------------------------------------
/*
A JOIN operation combines rows from two tables (or other table-like sources, such as views or table functions) to create a new combined row that can be used in the query.

-- Syntax
-- ON based
SELECT ...
FROM <object_ref1> [
                     {
                       INNER
                       | { LEFT | RIGHT | FULL } [ OUTER ]
                     }
                   ]
                   JOIN <object_ref2>
  [ ON <condition> ]
[ ... ]


-- USING based
SELECT *
FROM <object_ref1> [
                     {
                       INNER
                       | { LEFT | RIGHT | FULL } [ OUTER ]
                     }
                   ]
                   JOIN <object_ref2>
  [ USING( <column_list> ) ]
[ ... ]


-- Without any refernce_key
SELECT ...
FROM <object_ref1> [
                     {
                       | NATURAL [ { LEFT | RIGHT | FULL } [ OUTER ] ]
                       | CROSS
                     }
                   ]
                   JOIN <object_ref2>
[ ... ]
*/
-- JOIN EXAMPLE
CREATE TABLE t1 (col1 INTEGER);
CREATE TABLE t2 (col1 INTEGER);

INSERT INTO t1 (col1) VALUES 
   (2),
   (3),
   (4);
INSERT INTO t2 (col1) VALUES 
   (1),
   (2),
   (2),
   (3);
   

SELECT t1.col1, t2.col1
FROM t1 INNER JOIN t2
ON t2.col1 = t1.col1
ORDER BY 1,2;


SELECT t1.col1, t2.col1
FROM t1 CROSS JOIN t2
ORDER BY 1, 2;



-- New Topic: NATURAL INNER JOIN Example
CREATE OR REPLACE TABLE d1 (
  id number,
  name string
  );
INSERT INTO d1 (id, name) VALUES
  (1,'a'),
  (2,'b'),
  (4,'c');

CREATE OR REPLACE TABLE d2 (
  id number,
  value string
  );
INSERT INTO d2 (id, value) VALUES
  (1,'xx'),
  (2,'yy'),
  (5,'zz');

SELECT *
FROM d1 NATURAL INNER JOIN d2	-- Automatically join on id column and return only 1 id column
ORDER BY id;

-- New Topic: JOIN with USING
WITH
    l AS (
         SELECT 'a' AS userid
         ),
    r AS (
         SELECT 'b' AS userid
         )
  SELECT *
    FROM l LEFT JOIN r USING(userid)
	
	
	
-----------------------------------------New Topic:ASOF JOIN Statement:-----------------------------------------
/*
An ASOF JOIN operation combines rows from two tables based on timestamp values that follow each other, precede each other, or match exactly. For each row in the first (or left) table, the join finds a single row in the second (or right) table that has the closest timestamp value. The qualifying row on the right side is the closest match, which could be equal in time, earlier in time, or later in time, depending on the specified comparison operator.

-- Syntax
FROM <left_table> ASOF JOIN <right_table>
  MATCH_CONDITION ( <left_table.timecol> <comparison_operator> <right_table.timecol> )
  [ ON <table.col> = <table.col> [ AND ... ] | USING ( <column_list> ) ]
  
*/

-- Setting up the data for the examples
CREATE OR REPLACE TABLE left_table (
  c1 VARCHAR(1),
  c2 TINYINT,
  c3 TIME,
  c4 NUMBER(3,2)
);

CREATE OR REPLACE TABLE right_table (
  c1 VARCHAR(1),
  c2 TINYINT,
  c3 TIME,
  c4 NUMBER(3,2)
);

INSERT INTO left_table VALUES
  ('A',1,'09:15:00',3.21),
  ('A',2,'09:16:00',3.22),
  ('B',1,'09:17:00',3.23),
  ('B',2,'09:18:00',4.23);

INSERT INTO right_table VALUES
  ('A',1,'09:14:00',3.19),
  ('B',1,'09:16:00',3.04);
  

SELECT *
  FROM left_table l ASOF JOIN right_table r
    MATCH_CONDITION(l.c3>=r.c3)
    -- ON(l.c1=r.c1 and l.c2=r.c2) -- Optional for asof join 
  ORDER BY l.c1, l.c2;
  
  
  
-----------------------------------------New Topic:Lateral join Statement:-----------------------------------------
/*
In a FROM clause, the LATERAL keyword allows an inline view to reference columns from a table expression that precedes that inline view.

-- Syntax
SELECT ...
FROM <left_hand_table_expression>, LATERAL ( <inline_view> )
...
*/

-- Examples
CREATE TABLE departments (department_id INTEGER, name VARCHAR);
CREATE TABLE employees (employee_ID INTEGER, last_name VARCHAR, 
                        department_ID INTEGER, project_names ARRAY);
						
INSERT INTO departments (department_ID, name) VALUES 
    (1, 'Engineering'), 
    (2, 'Support');
INSERT INTO employees (employee_ID, last_name, department_ID) VALUES 
    (101, 'Richards', 1),
    (102, 'Paulson',  1),
    (103, 'Johnson',  2);
	
	
-- This example shows a LATERAL JOIN with a subquery
SELECT * 
    FROM departments AS d, LATERAL (SELECT * FROM employees AS e WHERE e.department_ID = d.department_ID) AS iv2
    ORDER BY employee_ID;
	
	
-- Example of using LATERAL with FLATTEN()
UPDATE employees SET project_names = ARRAY_CONSTRUCT('Materialized Views', 'UDFs') 
    WHERE employee_ID = 101;
UPDATE employees SET project_names = ARRAY_CONSTRUCT('Materialized Views', 'Lateral Joins')
    WHERE employee_ID = 102;
	

SELECT emp.employee_ID, emp.last_name, index, value AS project_name
    FROM employees AS emp, LATERAL FLATTEN(INPUT => emp.project_names) AS proj_names
    ORDER BY employee_ID;
	
	
-----------------------------------------New Topic:MATCH_RECOGNIZE Statement:-----------------------------------------
/*
Recognizes matches of a pattern in a set of rows. MATCH_RECOGNIZE accepts a set of rows (from a table, view, subquery, or other source) as input, and returns all matches for a given row pattern within this set. The pattern is defined similarly to a regular expression.

-- Syntax
MATCH_RECOGNIZE (
    [ PARTITION BY <expr> [, ... ] ]
    [ ORDER BY <expr> [, ... ] ]
    [ MEASURES <expr> [AS] <alias> [, ... ] ]
    [ ONE ROW PER MATCH |
      ALL ROWS PER MATCH [ { SHOW EMPTY MATCHES | OMIT EMPTY MATCHES | WITH UNMATCHED ROWS } ]
      ]
    [ AFTER MATCH SKIP
          {
          PAST LAST ROW   |
          TO NEXT ROW   |
          TO [ { FIRST | LAST} ] <symbol>
          }
      ]
    PATTERN ( <pattern> )
    DEFINE <symbol> AS <expr> [, ... ]
)
*/

create table stock_price_history (company TEXT, price_date DATE, price INT);

insert into stock_price_history values
    ('ABCD', '2020-10-01', 50),
    ('XYZ' , '2020-10-01', 89),
    ('ABCD', '2020-10-02', 36),
    ('XYZ' , '2020-10-02', 24),
    ('ABCD', '2020-10-03', 39),
    ('XYZ' , '2020-10-03', 37),
    ('ABCD', '2020-10-04', 42),
    ('XYZ' , '2020-10-04', 63),
    ('ABCD', '2020-10-05', 30),
    ('XYZ' , '2020-10-05', 65),
    ('ABCD', '2020-10-06', 47),
    ('XYZ' , '2020-10-06', 56),
    ('ABCD', '2020-10-07', 71),
    ('XYZ' , '2020-10-07', 50),
    ('ABCD', '2020-10-08', 80),
    ('XYZ' , '2020-10-08', 54),
    ('ABCD', '2020-10-09', 75),
    ('XYZ' , '2020-10-09', 30),
    ('ABCD', '2020-10-10', 63),
    ('XYZ' , '2020-10-10', 32);
	

-- Report one summary row for each V shape
SELECT * FROM stock_price_history
  MATCH_RECOGNIZE(
    PARTITION BY company
    ORDER BY price_date
    MEASURES
      MATCH_NUMBER() AS match_number,
      FIRST(price_date) AS start_date,
      LAST(price_date) AS end_date,
      COUNT(*) AS rows_in_sequence,
      COUNT(row_with_price_decrease.*) AS num_decreases,
      COUNT(row_with_price_increase.*) AS num_increases
    ONE ROW PER MATCH
    AFTER MATCH SKIP TO LAST row_with_price_increase
    PATTERN(row_before_decrease row_with_price_decrease+ row_with_price_increase+)
    DEFINE
      row_with_price_decrease AS price < LAG(price),
      row_with_price_increase AS price > LAG(price)
  )
ORDER BY company, match_number;

-- Report all rows for all matches for one company
select price_date, match_number, msq, price, cl from
  (select * from stock_price_history where company='ABCD') match_recognize(
    order by price_date
    measures
        match_number() as "MATCH_NUMBER",
        match_sequence_number() as msq,
        classifier() as cl
    all rows per match
    pattern(ANY_ROW UP+)
    define
        ANY_ROW AS TRUE,
        UP as price > lag(price)
)
order by match_number, msq;

-- Omit empty matches
select * from stock_price_history match_recognize(
    partition by company
    order by price_date
    measures
        match_number() as "MATCH_NUMBER"
    all rows per match omit empty matches
    pattern(OVERAVG*)
    define
        OVERAVG as price > avg(price) over (rows between unbounded
                                  preceding and unbounded following)
)
order by company, price_date;

-- Demonstrate the WITH UNMATCHED ROWS option
select * from stock_price_history match_recognize(
    partition by company
    order by price_date
    measures
        match_number() as "MATCH_NUMBER",
        classifier() as cl
    all rows per match with unmatched rows
    pattern(OVERAVG+)
    define
        OVERAVG as price > avg(price) over (rows between unbounded
                                 preceding and unbounded following)
)
order by company, price_date;

-- Demonstrate symbol predicates in the MEASURES clause
SELECT company, price_date, price, "FINAL FIRST(LT45.price)", "FINAL LAST(LT45.price)"
    FROM stock_price_history
       MATCH_RECOGNIZE (
           PARTITION BY company
           ORDER BY price_date
           MEASURES
               FINAL FIRST(LT45.price) AS "FINAL FIRST(LT45.price)",
               FINAL LAST(LT45.price)  AS "FINAL LAST(LT45.price)"
           ALL ROWS PER MATCH
           AFTER MATCH SKIP PAST LAST ROW
           PATTERN (LT45 LT45)
           DEFINE
               LT45 AS price < 45.00
           )
    WHERE company = 'ABCD'
    ORDER BY price_date;
	
	

-----------------------------------------New Topic:PIVOT Statement:-----------------------------------------
/*
Rotates a table by turning the unique values from one column in the input expression into multiple columns and aggregating results where required on any remaining column values. In a query, it is specified in the FROM clause after the table name or subquery.

The operator supports the built-in aggregate functions AVG, COUNT, MAX, MIN, and SUM.

PIVOT can be used to transform a narrow table (e.g. empid, month, sales) into a wider table (e.g. empid, jan_sales, feb_sales, mar_sales).

-- Syntax
SELECT ...
FROM ...
   PIVOT ( <aggregate_function> ( <pivot_column> )
            FOR <value_column> IN (
              <pivot_value_1> [ , <pivot_value_2> ... ]
              | ANY [ ORDER BY ... ]
              | <subquery>
            )
            [ DEFAULT ON NULL (<value>) ]
         )

[ ... ]

*/

CREATE OR REPLACE TABLE quarterly_sales(
  empid INT, 
  amount INT, 
  quarter TEXT)
  AS SELECT * FROM VALUES
    (1, 10000, '2023_Q1'),
    (1, 400, '2023_Q1'),
    (2, 4500, '2023_Q1'),
    (2, 35000, '2023_Q1'),
    (1, 5000, '2023_Q2'),
    (1, 3000, '2023_Q2'),
    (2, 200, '2023_Q2'),
    (2, 90500, '2023_Q2'),
    (1, 6000, '2023_Q3'),
    (1, 5000, '2023_Q3'),
    (2, 2500, '2023_Q3'),
    (2, 9500, '2023_Q3'),
    (1, 8000, '2023_Q4'),
    (1, 10000, '2023_Q4'),
    (2, 800, '2023_Q4'),
    (2, 4500, '2023_Q4');
	
-- Pivot on all distinct column values automatically with dynamic pivot
SELECT *
  FROM quarterly_sales
    PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter))
  ORDER BY empid;
  
CREATE OR REPLACE TABLE ad_campaign_types_by_quarter(
  quarter VARCHAR,
  television BOOLEAN,
  radio BOOLEAN,
  print BOOLEAN)
  AS SELECT * FROM VALUES
    ('2023_Q1', TRUE, FALSE, FALSE),
    ('2023_Q2', FALSE, TRUE, TRUE),
    ('2023_Q3', FALSE, TRUE, FALSE),
    ('2023_Q4', TRUE, FALSE, TRUE);
	
-- Pivot on column values using a subquery with dynamic pivot
SELECT *
  FROM quarterly_sales
    PIVOT(SUM(amount) FOR quarter IN (
      SELECT DISTINCT quarter
        FROM ad_campaign_types_by_quarter
        WHERE television = TRUE
        ORDER BY quarter)
    )
  ORDER BY empid;
  
-- Pivot on a specified list of column values for the pivot column
SELECT *
  FROM quarterly_sales
    PIVOT(SUM(amount) FOR quarter IN (
      '2023_Q1',
      '2023_Q2',
      '2023_Q3')
    )
  ORDER BY empid;
  

SELECT * 
  FROM quarterly_sales
    PIVOT(SUM(amount) FOR quarter IN (
      '2023_Q1', 
      '2023_Q2', 
      '2023_Q3', 
      '2023_Q4')
    ) AS p (empid_renamed, Q1, Q2, Q3, Q4) -- Renamed pivoted COLUMNS
  ORDER BY empid_renamed;
  
  

-----------------------------------------New Topic:UNPIVOT Statement:-----------------------------------------
/*
Rotates a table by transforming columns into rows. UNPIVOT is a relational operator that accepts two columns (from a table or subquery), along with a list of columns, and generates a row for each column specified in the list. In a query, it is specified in the FROM clause after the table name or subquery.

UNPIVOT is not exactly the reverse of PIVOT because it cannot undo aggregations made by PIVOT.

This operator can be used to transform a wide table (e.g. empid, jan_sales, feb_sales, mar_sales) into a narrower table (e.g. empid, month, sales).

-- Syntax
SELECT ...
FROM ...
   UNPIVOT [ { INCLUDE | EXCLUDE } NULLS ]
     ( <value_column>
       FOR <name_column> IN ( <column_list> ) )

[ ... ]
*/

CREATE OR REPLACE TABLE monthly_sales(
  empid INT,
  dept TEXT,
  jan INT,
  feb INT,
  mar INT,
  april INT
);

INSERT INTO monthly_sales VALUES
  (1, 'electronics', 100, 200, 300, 100),
  (2, 'clothes', 100, 300, 150, 200),
  (3, 'cars', 200, 400, 100, 50),
  (4, 'appliances', 100, NULL, 100, 50);

SELECT * FROM monthly_sales;

SELECT *
  FROM monthly_sales
    UNPIVOT INCLUDE NULLS (sales FOR month IN (jan, feb, mar, april))
  ORDER BY empid;
  
 

-----------------------------------------:VALUES Statement:-----------------------------------------
SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three'));

SELECT column1, $2 FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three'));



-----------------------------------------New Topic:SAMPLE / TABLESAMPLE Statement:-----------------------------------------
/*
Returns a subset of rows sampled randomly from the specified table. The following sampling methods are supported:

Sample a fraction of a table, with a specified probability for including a given row. The number of rows returned depends on the size of the table and the requested probability. A seed can be specified to make the sampling deterministic.

Sample a fixed, specified number of rows. The exact number of specified rows is returned unless the table contains fewer rows.

SAMPLE and TABLESAMPLE are synonymous and can be used interchangeably.

-- Syntax
SELECT ...
FROM ...
  { SAMPLE | TABLESAMPLE } [ samplingMethod ] ( { <probability> | <num> ROWS } ) [ { REPEATABLE | SEED } ( <seed> ) ]
[ ... ]
where:
samplingMethod ::= { { BERNOULLI | ROW } |
                     { SYSTEM | BLOCK } }
*/

-- Return a sample of a table in which each row has a 10% probability of being included in the sample
SELECT * FROM testtable SAMPLE (10);

-- Return a sample of a table in which each row has a 20.3% probability of being included in the sample:
SELECT * FROM testtable TABLESAMPLE BERNOULLI (20.3);

-- Return an empty sample
SELECT * FROM testtable SAMPLE ROW (0);

-- This example shows how to sample multiple tables in a join
SELECT i, j
    FROM
         table1 AS t1 SAMPLE (25)     -- 25% of rows in table1
             INNER JOIN
         table2 AS t2 SAMPLE (50)     -- 50% of rows in table2
    WHERE t2.j = t1.i
    ;
	
-- Fraction-based block sampling (with seeds)
SELECT * FROM testtable SAMPLE SYSTEM (3) SEED (82);

SELECT * FROM testtable SAMPLE BLOCK (0.012) REPEATABLE (99992);

-- Fixed-size row sampling
SELECT * FROM testtable SAMPLE (10 ROWS);




-----------------------------------------:WHERE Statement:-----------------------------------------
SELECT column_x
   FROM mytable
   WHERE column_y IN (<expr1>, <expr2>, <expr3> ...);
   
SELECT t1.c1, t2.c2
FROM t1 LEFT OUTER JOIN t2
        ON t1.c1 = t2.c2;

SELECT t1.c1, t2.c2
FROM t1, t2
WHERE t1.c1 = t2.c2(+);	-- the (+) is on the right hand side and identifies the inner table



-----------------------------------------:GROUP BY Statement:-----------------------------------------
SELECT SUM(amount)
  FROM mytable
  GROUP BY ALL;
  
-- EXAMPLE
CREATE TABLE sales (
  product_ID INTEGER,
  retail_price REAL,
  quantity INTEGER,
  city VARCHAR,
  state VARCHAR);

INSERT INTO sales (product_id, retail_price, quantity, city, state) VALUES
  (1, 2.00,  1, 'SF', 'CA'),
  (1, 2.00,  2, 'SJ', 'CA'),
  (2, 5.00,  4, 'SF', 'CA'),
  (2, 5.00,  8, 'SJ', 'CA'),
  (2, 5.00, 16, 'Miami', 'FL'),
  (2, 5.00, 32, 'Orlando', 'FL'),
  (2, 5.00, 64, 'SJ', 'PR');

CREATE TABLE products (
  product_ID INTEGER,
  wholesale_price REAL);
INSERT INTO products (product_ID, wholesale_price) VALUES (1, 1.00);
INSERT INTO products (product_ID, wholesale_price) VALUES (2, 2.00);


SELECT state, city, SUM(retail_price * quantity) AS gross_revenue
  FROM sales
  GROUP BY state, city;
  
  

-----------------------------------------New Topic:GROUP BY CUBE Statement:-----------------------------------------
/*
GROUP BY CUBE is an extension of the GROUP BY clause similar to GROUP BY ROLLUP. In addition to producing all the rows of a GROUP BY ROLLUP, GROUP BY CUBE adds all the “cross-tabulations” rows. Sub-total rows are rows that further aggregate whose values are derived by computing the same aggregate functions that were used to produce the grouped rows.

A CUBE grouping is equivalent to a series of grouping sets and is essentially a shorter specification. The N elements of a CUBE specification correspond to 2^N GROUPING SETS.

-- Syntax
SELECT ...
FROM ...
[ ... ]
GROUP BY CUBE ( groupCube [ , groupCube [ , ... ] ] )
[ ... ]
where:
groupCube ::= { <column_alias> | <position> | <expr> }

*/
-- Create some tables and insert some rows.
CREATE TABLE products (product_ID INTEGER, wholesale_price REAL);
INSERT INTO products (product_ID, wholesale_price) VALUES 
    (1, 1.00),
    (2, 2.00);

CREATE TABLE sales (product_ID INTEGER, retail_price REAL, 
    quantity INTEGER, city VARCHAR, state VARCHAR);
INSERT INTO sales (product_id, retail_price, quantity, city, state) VALUES 
    (1, 2.00,  1, 'SF', 'CA'),
    (1, 2.00,  2, 'SJ', 'CA'),
    (2, 5.00,  4, 'SF', 'CA'),
    (2, 5.00,  8, 'SJ', 'CA'),
    (2, 5.00, 16, 'Miami', 'FL'),
    (2, 5.00, 32, 'Orlando', 'FL'),
    (2, 5.00, 64, 'SJ', 'PR');
	
SELECT state, city, SUM((s.retail_price - p.wholesale_price) * s.quantity) AS profit 
 FROM products AS p, sales AS s
 WHERE s.product_ID = p.product_ID
 GROUP BY CUBE (state, city)
 ORDER BY state, city NULLS LAST
 ;
 
 
 
 
-----------------------------------------New Topic:GROUP BY GROUPING SETS Statement:-----------------------------------------
/*
GROUP BY GROUPING SETS is a powerful extension of the GROUP BY clause that computes multiple group-by clauses in a single statement. The group set is a set of dimension columns.

GROUP BY GROUPING SETS is equivalent to the UNION of two or more GROUP BY operations in the same result set:
	* GROUP BY GROUPING SETS(a) is equivalent to the single grouping set operation GROUP BY a.
	* GROUP BY GROUPING SETS(a,b) is equivalent to GROUP BY a UNION ALL GROUP BY b.

-- Syntax
SELECT ...
FROM ...
[ ... ]
GROUP BY GROUPING SETS ( groupSet [ , groupSet [ , ... ] ] )
[ ... ]

where:
groupSet ::= { <column_alias> | <position> | <expr> }
*/

CREATE or replace TABLE nurses (
  ID INTEGER,
  full_name VARCHAR,
  medical_license VARCHAR,   -- LVN, RN, etc.
  radio_license VARCHAR      -- Technician, General, Amateur Extra
  )
  ;

INSERT INTO nurses
    (ID, full_name, medical_license, radio_license)
  VALUES
    (201, 'Thomas Leonard Vicente', 'LVN', 'Technician'),
    (202, 'Tamara Lolita VanZant', 'LVN', 'Technician'),
    (341, 'Georgeann Linda Vente', 'LVN', 'General'),
    (471, 'Andrea Renee Nouveau', 'RN', 'Amateur Extra')
    ;
	
SELECT COUNT(*), medical_license, radio_license
  FROM nurses
  GROUP BY GROUPING SETS (medical_license, radio_license);
  
  
  
-----------------------------------------New Topic:GROUP BY GROUPING SETS Statement:-----------------------------------------
/*
GROUP BY ROLLUP is an extension of the GROUP BY clause that produces sub-total rows (in addition to the grouped rows). Sub-total rows are rows that further aggregate whose values are derived by computing the same aggregate functions that were used to produce the grouped rows.

You can think of rollup as generating multiple result sets, each of which (after the first) is the aggregate of the previous result set. So, for example, if you own a chain of retail stores, you might want to see the profit for:
	* Each store.
	* Each city (large cities might have multiple stores).
	* Each state.
	* Everything (all stores in all states).
	
-- Syntax
SELECT ...
FROM ...
[ ... ]
GROUP BY ROLLUP ( groupRollup [ , groupRollup [ , ... ] ] )
[ ... ]

where:
groupRollup ::= { <column_alias> | <position> | <expr> }
*/
-- Create some tables and insert some rows.
CREATE TABLE products (product_ID INTEGER, wholesale_price REAL);
INSERT INTO products (product_ID, wholesale_price) VALUES 
    (1, 1.00),
    (2, 2.00);

CREATE TABLE sales (product_ID INTEGER, retail_price REAL, 
    quantity INTEGER, city VARCHAR, state VARCHAR);
INSERT INTO sales (product_id, retail_price, quantity, city, state) VALUES 
    (1, 2.00,  1, 'SF', 'CA'),
    (1, 2.00,  2, 'SJ', 'CA'),
    (2, 5.00,  4, 'SF', 'CA'),
    (2, 5.00,  8, 'SJ', 'CA'),
    (2, 5.00, 16, 'Miami', 'FL'),
    (2, 5.00, 32, 'Orlando', 'FL'),
    (2, 5.00, 64, 'SJ', 'PR');
	
SELECT state, city, SUM((s.retail_price - p.wholesale_price) * s.quantity) AS profit 
FROM products AS p, sales AS s
WHERE s.product_ID = p.product_ID
GROUP BY ROLLUP (state, city)
ORDER BY state, city NULLS LAST



-----------------------------------------:HAVING Statement:-----------------------------------------
SELECT department_id
FROM employees
GROUP BY department_id
HAVING count(*) < 10;


-----------------------------------------New Topic:QUALIFY Statement:-----------------------------------------
/*
In a SELECT statement, the QUALIFY clause filters the results of window functions.

QUALIFY does with window functions what HAVING does with aggregate functions and GROUP BY clauses.

In the execution order of a query, QUALIFY is therefore evaluated after window functions are computed. Typically, a SELECT statement’s clauses are evaluated in the order shown below:
	* From
	* Where
	* Group by
	* Having
	* Window
	* QUALIFY
	* Distinct
	* Order by
	* Limit

-- Syntax
SELECT <column_list>
  FROM <data_source>
  [GROUP BY ...]
  [HAVING ...]
  QUALIFY <predicate>
  [ ... ]
*/

CREATE TABLE qt (i INTEGER, p CHAR(1), o INTEGER);
INSERT INTO qt (i, p, o) VALUES
    (1, 'A', 1),
    (2, 'A', 2),
    (3, 'B', 1),
    (4, 'B', 2);
	
	
SELECT i, p, o
    FROM qt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1;



-----------------------------------------:ORDER BY Statement:-----------------------------------------
SELECT column1
  FROM VALUES ('a'), ('1'), ('B'), (null), ('2'), ('01'), ('05'), (' this'), ('this'), ('this and that'), ('&'), ('%')
  ORDER BY column1;
  
  
SELECT column1
  FROM VALUES (1), (null), (2), (null), (3)
  ORDER BY column1 NULLS FIRST;
  
 
SELECT column1
  FROM VALUES (1), (null), (2), (null), (3)
  ORDER BY column1 DESC NULLS LAST;
  
  

-----------------------------------------:LIMIT / FETCH Statement:-----------------------------------------
CREATE TABLE demo1 (i INTEGER);
INSERT INTO demo1 (i) VALUES (1), (2);

select c1 from testtable order by c1 limit 3 offset 3;




-----------------------------------------New Topic:FOR UPDATE Statement:-----------------------------------------
/*
Locks the rows that the query selects until the transaction that contains the query commits or aborts.

This clause is supported for use with hybrid tables only, and is useful for transactional workloads in which multiple transactions attempt to access the same rows at the same time. Rows are locked for update in the sense that other transactions cannot write data to these rows until the transaction doing the locking has been fully committed or rolled back. However, other transactions can read the locked rows, and other rows in the same table can be read, updated, or deleted.

-- Syntax
SELECT ...
  FROM ...
  [ ... ]
  FOR UPDATE [ NOWAIT | WAIT <wait_time> ]
*/

BEGIN;
...
SELECT * FROM ht ORDER BY c1 FOR UPDATE;
...
UPDATE ht set c1 = c1 + 10 WHERE c1 = 0;
...
SELECT ... ;
...
COMMIT;


-- Apply a maximum wait time of 60 seconds for row locking
BEGIN;
...
SELECT * FROM ht FOR UPDATE WAIT 60;
...
COMMIT;
