CREATE DATABASE IF NOT EXISTS SNOWFLAKE_TUTORIALS;
USE SNOWFLAKE_TUTORIALS;
CREATE SCHEMA IF NOT EXISTS LEARNING;
USE SCHEMA LEARNING;


/*
Variable Declaration:
	
Variables can only be declaired inside the block statement. There are 2 ways to declaire variables:
1. In DECLARE block
2. In BEGIN block
*/

EXECUTE IMMEDIATE 
$$
DECLARE		-- DECLARE block (without LET)
  profit number(38, 2) DEFAULT 0.0; 
BEGIN		-- BEGIN block (with LET)
  LET cost number(38, 2) := 100.0;
  LET revenue number(38, 2) DEFAULT 110.0;

  profit := revenue - cost;
  RETURN profit;
END;
$$
;

/*
Data Types:

Snowflake supports most SQL data types:

Category								Type												Notes

Numeric data types						NUMBER												Default precision and scale are (38,0).
										DECIMAL, NUMERIC									Synonymous with NUMBER.
										INT, INTEGER, BIGINT, SMALLINT, TINYINT, BYTEINT	Synonymous with NUMBER except precision and scale cannot be specified.
										FLOAT, FLOAT4, FLOAT8
										DOUBLE, DOUBLE PRECISION, REAL						Synonymous with FLOAT.
												
String & binary data types				VARCHAR												Default (and maximum) is 16,777,216 bytes.
										CHAR, CHARACTER										Synonymous with VARCHAR except default length is VARCHAR(1).
										STRING												Synonymous with VARCHAR.
										TEXT												Synonymous with VARCHAR.
										BINARY
										VARBINARY											Synonymous with BINARY.

Logical data types						BOOLEAN												Currently only supported for accounts provisioned after January 25, 2016.

Date & time data types					DATE
										DATETIME											Alias for TIMESTAMP_NTZ
										TIME
										TIMESTAMP											Alias for one of the TIMESTAMP variations (TIMESTAMP_NTZ by default).
										TIMESTAMP_LTZ										TIMESTAMP with local time zone; time zone, if provided, is not stored.
										TIMESTAMP_NTZ										TIMESTAMP with no time zone; time zone, if provided, is not stored.
										TIMESTAMP_TZ										TIMESTAMP with time zone.

Semi-structured data types				VARIANT
										OBJECT
										ARRAY

Geospatial data types					GEOGRAPHY
										GEOMETRY

Vector data types						VECTOR
*/

-- 1. Numeric Data Types
CREATE OR REPLACE TABLE numeric_table_example(
	numer_col NUMBER,
	number_10_1_col NUMBER(10, 1),
	decimal_col DECIMAL(20, 2),
	numeric_col NUMERIC(20, 2),
	integer_col INTEGER,
	int_col INT,
	double_col DOUBLE,
	float_col FLOAT,
	double_precision_col DOUBLE PRECISION,
	real_col REAL
);

DESC TABLE numeric_table_example;


-- 2. String & Binary Data Types:

-- 2.1 String Data Types
CREATE OR REPLACE TABLE varchar_table_example(
	varchar_col VARCHAR,
	varchar_50_col VARCHAR(50),
	char_col CHAR,
	char_10_col CHAR(10),
	string_col STRING,
	string_20_col STRING(20),
	text_col TEXT,
	text_30_col TEXT(30)
);

DESC TABLE varchar_table_example;

-- 2.2 Binary Data Types
CREATE OR REPLACE TABLE binary_table_example(
	binary_col BINARY,
	binary_100_col BINARY(100),
	vabinary_col VARBINARY
);

DESC TABLE binary_table_example;


-- 3. Logica Data Types
CREATE OR REPLACE TABLE boolean_table_example(
	boolean_col BOOLEAN,
	number_col NUMBER,
	string_col STRING
);

INSERT INTO boolean_table_example VALUES (true, 1, 'yes'), (false, 0, 'no'), (null, null, null);

SELECT * FROM boolean_table_example;

-- Text cast to boolean
SELECT string_col, TO_BOOLEAN(string_col) FROM boolean_table_example;
	

-- 4. Date & Time Data Types
CREATE OR REPLACE TABLE date_time_table_example(
	date_col DATE,
	datetime_col TIMESTAMP,
	datetime_ltz_col TIMESTAMP_LTZ,		-- TIMESTAMP WITH LOCAL TIME ZONE
	datetime_ntz_col TIMESTAMP_NTZ,		-- TIMESTAMP WITHOUT TIME ZONE
	datetime_tz_col TIMESTAMP_TZ		-- TIMESTAMP WITH TIME ZONE
);

DESC TABLE date_time_table_example;

INSERT INTO date_time_table_example VALUES('2014-01-02 16:00:00 +00:00','2014-01-02 16:00:00 +00:00','2014-01-02 16:00:00 +00:00','2014-01-02 16:00:00 +00:00','2014-01-02 16:00:00 +00:00');

SELECT * FROM date_time_table_example;


-- 5. Semi-Structured Data Types
CREATE OR REPLACE TABLE semi_structure_table_example(
	float_col FLOAT,
	variant_col VARIANT,
	array_col ARRAY,
	object_col OBJECT
);

DESC TABLE semi_structure_table_example;

INSERT INTO semi_structure_table_example SELECT
1.23, to_variant(1.23), OBJECT_CONSTRUCT('name', 'Jones','age',  42), ARRAY_CONSTRUCT(1, 2);

INSERT INTO semi_structure_table_example SELECT
1.23, [1,2,3], {'name': 'Ram','age':  20}, [1,2,3,4];

INSERT INTO semi_structure_table_example SELECT
1.23, {'key1': 'val1','key2':  'val2'}, {'name': 'Manoj','age':  30}, ['a', 1, 'b'];

SELECT * FROM semi_structure_table_example;


-- 6. Structured Data Types
SELECT
	SYSTEM$TYPEOF(
		{
			'str': 'test',
			'num': 1
		}::OBJECT(
			str VARCHAR NOT NULL,
			num NUMBER
		)
	) AS structured_object,
	SYSTEM$TYPEOF(
		{
			'str': 'test',
			'num': 1
		}
	) AS semi_structured_object;
	
-- Specifying a MAP
SELECT
  SYSTEM$TYPEOF(
    {
      'a_key': 'a_val',
      'b_key': 'b_val'
    }::MAP(VARCHAR, VARCHAR)
  ) AS map_example;
	

-- 7. Geospatial Data Types
CREATE OR REPLACE TABLE geospatial_table_example (id INTEGER, geography_col GEOGRAPHY);

INSERT INTO geospatial_table_example VALUES
    (1, 'POINT(-122.35 37.55)'), (2, 'LINESTRING(-124.20 42.00, -120.01 41.99)');
	
ALTER SESSION SET GEOGRAPHY_OUTPUT_FORMAT='GeoJSON';
SELECT geography_col FROM geospatial_table_example ORDER BY ID;

alter session set GEOGRAPHY_OUTPUT_FORMAT='WKT';
SELECT geography_col FROM geospatial_table_example ORDER BY ID;

alter session set GEOGRAPHY_OUTPUT_FORMAT='WKB';
SELECT geography_col FROM geospatial_table_example ORDER BY ID;

alter session set GEOGRAPHY_OUTPUT_FORMAT='EWKT';
SELECT geography_col FROM geospatial_table_example ORDER BY ID;

alter session set GEOGRAPHY_OUTPUT_FORMAT='EWKB';
SELECT geography_col FROM geospatial_table_example ORDER BY ID;



-- 8. Vector Data Types
CREATE OR REPLACE TABLE vector_table_example (a VECTOR(float, 3), b VECTOR(float, 3));
INSERT INTO vector_table_example SELECT [1.1,2.2,3]::VECTOR(FLOAT,3), [1,1,1]::VECTOR(FLOAT,3);
INSERT INTO vector_table_example SELECT [1,2.2,3]::VECTOR(FLOAT,3), [4,6,8]::VECTOR(FLOAT,3);
SELECT * FROM vector_table_example;


-- *** Unsupported Data Types
/*
Category				Type				Notes
LOB (Large Object)		BLOB				BINARY can be used instead; maximum of 8,388,608 bytes. For more information, see String & binary data types.
						CLOB				VARCHAR can be used instead; maximum of 16,777,216 bytes (for singlebyte). For more information, see String & binary data types.

Other					ENUM
						User-defined data types
*/

-- *** Data Type Conversion:

-- 1. Explicit Casting
SELECT CAST('2022-04-01' AS DATE);
SELECT '2022-04-01'::DATE;
SELECT TO_DATE('2022-04-01');

-- 2. Implicit Casting (“coercion”): Coercion occurs when a function (or operator) requires a data type that is different from, but compatible with, the arguments (or operands).
SELECT 17 || '76';