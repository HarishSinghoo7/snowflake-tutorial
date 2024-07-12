CREATE DATABASE IF NOT EXISTS SNOWFLAKE_TUTORIALS;
USE SNOWFLAKE_TUTORIALS;
CREATE SCHEMA IF NOT EXISTS TUTORIAL_3;
USE SCHEMA TUTORIAL_3;


-----------------------------------------:Normal SQL TABLE:-----------------------------------------
/*
-- Syntax

CREATE [ OR REPLACE ]
    [ { [ { LOCAL | GLOBAL } ] TEMP | TEMPORARY | VOLATILE | TRANSIENT } ]
  TABLE [ IF NOT EXISTS ] <table_name> (
    -- Column definition
    <col_name> <col_type>
      [ inlineConstraint ]
      [ NOT NULL ]
      [ COLLATE '<collation_specification>' ]
      [
        {
          DEFAULT <expr>
          | { AUTOINCREMENT | IDENTITY }
            [
              {
                ( <start_num> , <step_num> )
                | START <num> INCREMENT <num>
              }
            ]
            [ { ORDER | NOORDER } ]
        }
      ]
      [ [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ] ]
      [ [ WITH ] PROJECTION POLICY <policy_name> ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT '<string_literal>' ]

    -- Additional column definitions
    [ , <col_name> <col_type> [ ... ] ]

    -- Out-of-line constraints
    [ , outoflineConstraint [ ... ] ]
  )
  [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
  [ ENABLE_SCHEMA_EVOLUTION = { TRUE | FALSE } ]
  [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
  [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
  [ CHANGE_TRACKING = { TRUE | FALSE } ]
  [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
  [ COPY GRANTS ]
  [ COMMENT = '<string_literal>' ]
  [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
  [ [ WITH ] AGGREGATION POLICY <policy_name> [ ENTITY KEY ( <col_name> [ , <col_name> ... ] ) ] ]
  [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
  
  
In addition, this command supports the following variants:
	* CREATE OR ALTER TABLE (creates a table if it doesn’t exist, or alters it according to the table definition)
	* CREATE TABLE … AS SELECT (creates a populated table; also referred to as CTAS)
	* CREATE TABLE … USING TEMPLATE (creates a table with the column definitions derived from a set of staged files)
	* CREATE TABLE … LIKE (creates an empty copy of an existing table)
	* CREATE TABLE … CLONE (creates a clone of an existing table)
*/

-- Create a simple table in the current database and insert a row in the table
CREATE TABLE mytable (amount NUMBER);

INSERT INTO mytable VALUES(1);

SHOW TABLES like 'mytable';

DESC TABLE mytable;


-- Create a simple table and specify comments for both the table and the column in the table
CREATE TABLE example (col1 NUMBER COMMENT 'a column comment') COMMENT='a table comment';

SHOW TABLES LIKE 'example';

DESC TABLE example;


-- Create a table by selecting from an existing table
CREATE TABLE mytable_copy (b) AS SELECT * FROM mytable;

DESC TABLE mytable_copy;

CREATE TABLE mytable_copy2 AS SELECT b+1 AS c FROM mytable_copy;

DESC TABLE mytable_copy2;

SELECT * FROM mytable_copy2;


-- Create a table by selecting columns from a staged Parquet data file
CREATE OR REPLACE TABLE parquet_col (
  custKey NUMBER DEFAULT NULL,
  orderDate DATE DEFAULT NULL,
  orderStatus VARCHAR(100) DEFAULT NULL,
  price VARCHAR(255)
)
AS SELECT
  $1:o_custkey::number,
  $1:o_orderdate::date,
  $1:o_orderstatus::text,
  $1:o_totalprice::text
FROM @my_stage;

DESC TABLE parquet_col;


-- Create a table with the same column definitions as another table, but with no rows
CREATE TABLE mytable (amount NUMBER);

INSERT INTO mytable VALUES(1);

SELECT * FROM mytable;

CREATE TABLE mytable_2 LIKE mytable;

DESC TABLE mytable_2;

SELECT * FROM mytable_2;


-- Create a table with a multi-column clustering key
CREATE TABLE mytable (date TIMESTAMP_NTZ, id NUMBER, content VARIANT) CLUSTER BY (date, id);

SHOW TABLES LIKE 'mytable';


-- Specify collation for columns in a table
CREATE TABLE collation_demo (
  uncollated_phrase VARCHAR, 
  utf8_phrase VARCHAR COLLATE 'utf8',
  english_phrase VARCHAR COLLATE 'en',
  spanish_phrase VARCHAR COLLATE 'sp'
  );

INSERT INTO collation_demo (uncollated_phrase, utf8_phrase, english_phrase, spanish_phrase) 
   VALUES ('pinata', 'pinata', 'pinata', 'piñata');
   
   
-- CREATE TABLE … USING TEMPLATE 
CREATE TABLE mytable
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    WITHIN GROUP (ORDER BY order_id)
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION=>'@mystage',
          FILE_FORMAT=>'my_parquet_format'
        )
      ));
	  
	  
-- Create a temporary table that is dropped automatically at the end of the session
CREATE [<LOCAL | GLOBAL> TEMPORARY | TEMP] TABLE demo_temporary (i INTEGER);
CREATE VOLATILE TABLE demo_volatile (i INTEGER);

-- Create a table my_table using the CREATE OR ALTER TABLE command
CREATE OR ALTER TABLE my_table(a INT);

CREATE OR ALTER TABLE my_table(
    a INT PRIMARY KEY,
    b VARCHAR(200)
  )
  DATA_RETENTION_TIME_IN_DAYS = 5
  DEFAULT_DDL_COLLATION = 'de';
  
 
CREATE TABLE table2 (
  col1 INTEGER NOT NULL,
  col2 INTEGER NOT NULL,
  CONSTRAINT pkey_1 PRIMARY KEY (col1, col2) NOT ENFORCED
);
CREATE TABLE table3 (
  col_a INTEGER NOT NULL,
  col_b INTEGER NOT NULL,
  CONSTRAINT fkey_1 FOREIGN KEY (col_a, col_b) REFERENCES table2 (col1, col2) NOT ENFORCED
);




-----------------------------------------:Dynamic TABLE:-----------------------------------------
/*
Dynamic tables simplify data engineering in Snowflake by providing a reliable, cost-effective, and automated way to transform data. Instead of managing transformation steps with tasks and scheduling, you define the end state using dynamic tables and let Snowflake handle the pipeline management.

Here’s why they’re beneficial:
	* Declarative programming: Define your pipeline outcomes using declarative SQL without worrying about the steps to achieve them, reducing complexity.
	* Transparent orchestration: Easily create pipelines of various shapes, from linear chains to directed graphs, by chaining dynamic tables together. Snowflake manages the orchestration and scheduling of pipeline refresh based on your data freshness target.
	* Performance boost with incremental processing: For favorable workloads that are suited for incremental processing, dynamic tables can provide a significant performance improvement over full refreshes.
	* Easy switching: Transition seamlessly from batch to streaming with a single ALTER DYNAMIC TABLE command. You control how often data is refreshed in your pipeline, which helps balance cost and data freshness.
	* Operationalization: Dynamic tables are fully observable and manageable through Snowsight, and also offer programmatic access to build your own observability apps.
	
-- Syntax
CREATE [ OR REPLACE ] [ TRANSIENT ] DYNAMIC TABLE [ IF NOT EXISTS ] <name> (
    -- Column definition
    <col_name> <col_type>
      [ [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ] ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT '<string_literal>' ]

    -- Additional column definitions
    [ , <col_name> <col_type> [ ... ] ]

  )
  TARGET_LAG = { '<num> { seconds | minutes | hours | days }' | DOWNSTREAM }
  WAREHOUSE = <warehouse_name>
  [ REFRESH_MODE = { AUTO | FULL | INCREMENTAL } ]
  [ INITIALIZE = { ON_CREATE | ON_SCHEDULE } ]
  [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
  [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
  [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
  [ COMMENT = '<string_literal>' ]
  [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
  [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
  AS <query>


*/
-- Create dynamic tables
CREATE OR REPLACE DYNAMIC TABLE product
  TARGET_LAG = '20 minutes'
  WAREHOUSE = mywh
  REFRESH_MODE = auto
  INITIALIZE = on_create
  AS
    SELECT product_id, product_name FROM staging_table;
	
	
-- Create dynamic Iceberg tables
CREATE DYNAMIC ICEBERG TABLE product (date TIMESTAMP_NTZ, id NUMBER, content STRING)
  TARGET_LAG = '20 minutes'
  WAREHOUSE = mywh
  EXTERNAL_VOLUME = 'my_external_volume'
  CATALOG = 'SNOWFLAKE'
  BASE_LOCATION = 'my_iceberg_table'
  AS
    SELECT product_id, product_name FROM staging_table;
	



-----------------------------------------:EVENT TABLE:-----------------------------------------
/*
An event table has the following characteristics that set it apart from other tables you create:
	* The table contains a predefined set of columns for capturing log entries and trace events. The table’s structure includes columns and key/value attribute object definitions to hold both predefined data and data you design.
	* You associate an event table with your account in order to capture log entries and trace events to that table. You can associate an account with only one event table at a time. The associated event table is referred to as the active event table.
	* You can specify the severity level of log messages and verbosity of trace events to capture.
	* Log messages and trace events generated by handler code for stored procedures, UDFs, and UDTFs are stored in the active event table.

-- Syntax

CREATE [ OR REPLACE ] EVENT TABLE [ IF NOT EXISTS ] <name>
  [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
  [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
  [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
  [ CHANGE_TRACKING = { TRUE | FALSE } ]
  [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
  [ COPY GRANTS ]
  [ [ WITH ] COMMENT = '<string_literal>' ]
  [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
  [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
*/

CREATE EVENT TABLE my_database.my_schema.my_events;

-- Check the link to see event table columns : https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-columns




-----------------------------------------:EXTERNAL TABLE:-----------------------------------------
/*
https://docs.snowflake.com/en/sql-reference/sql/create-external-table

An external table is a Snowflake feature that allows you to query data stored in an external stage as if the data were inside a table in Snowflake. The external stage is not part of Snowflake, so Snowflake does not store or manage the stage.

External tables let you store (within Snowflake) certain file-level metadata, including filenames, version identifiers, and related properties. External tables can access data stored in any format that the COPY INTO <table> command supports.

External tables are read-only. You cannot perform data manipulation language (DML) operations on them. However, you can use external tables for query and join operations. You can also create views against external tables.

Querying data in an external table might be slower than querying data that you store natively in a table within Snowflake. To improve query performance, you can use a materialized view based on an external table.

-- Syntax
-- Partitions computed from expressions
CREATE [ OR REPLACE ] EXTERNAL TABLE [IF NOT EXISTS]
  <table_name>
    ( [ <col_name> <col_type> AS <expr> | <part_col_name> <col_type> AS <part_expr> ]
      [ inlineConstraint ]
      [ , <col_name> <col_type> AS <expr> | <part_col_name> <col_type> AS <part_expr> ... ]
      [ , ... ] )
  cloudProviderParams
  [ PARTITION BY ( <part_col_name> [, <part_col_name> ... ] ) ]
  [ WITH ] LOCATION = externalStage
  [ REFRESH_ON_CREATE =  { TRUE | FALSE } ]
  [ AUTO_REFRESH = { TRUE | FALSE } ]
  [ PATTERN = '<regex_pattern>' ]
  FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET } [ formatTypeOptions ] } )
  [ AWS_SNS_TOPIC = '<string>' ]
  [ COPY GRANTS ]
  [ COMMENT = '<string_literal>' ]
  [ [ WITH ] ROW ACCESS POLICY <policy_name> ON (VALUE) ]
  [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

-- Partitions added and removed manually
CREATE [ OR REPLACE ] EXTERNAL TABLE [IF NOT EXISTS]
  <table_name>
    ( [ <col_name> <col_type> AS <expr> | <part_col_name> <col_type> AS <part_expr> ]
      [ inlineConstraint ]
      [ , <col_name> <col_type> AS <expr> | <part_col_name> <col_type> AS <part_expr> ... ]
      [ , ... ] )
  cloudProviderParams
  [ PARTITION BY ( <part_col_name> [, <part_col_name> ... ] ) ]
  [ WITH ] LOCATION = externalStage
  PARTITION_TYPE = USER_SPECIFIED
  FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET } [ formatTypeOptions ] } )
  [ COPY GRANTS ]
  [ COMMENT = '<string_literal>' ]
  [ [ WITH ] ROW ACCESS POLICY <policy_name> ON (VALUE) ]
  [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

-- Delta Lake
CREATE [ OR REPLACE ] EXTERNAL TABLE [IF NOT EXISTS]
  <table_name>
    ( [ <col_name> <col_type> AS <expr> | <part_col_name> <col_type> AS <part_expr> ]
      [ inlineConstraint ]
      [ , <col_name> <col_type> AS <expr> | <part_col_name> <col_type> AS <part_expr> ... ]
      [ , ... ] )
  cloudProviderParams
  [ PARTITION BY ( <part_col_name> [, <part_col_name> ... ] ) ]
  [ WITH ] LOCATION = externalStage
  PARTITION_TYPE = USER_SPECIFIED
  FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET } [ formatTypeOptions ] } )
  [ TABLE_FORMAT = DELTA ]
  [ COPY GRANTS ]
  [ COMMENT = '<string_literal>' ]
  [ [ WITH ] ROW ACCESS POLICY <policy_name> ON (VALUE) ]
  [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
*/
-- AWS S3 example
CREATE STAGE s1
  URL='s3://mybucket/files/logs/'
  ...
  ;

SELECT metadata$filename FROM @s1/;

CREATE EXTERNAL TABLE et1(
 date_part date AS TO_DATE(SPLIT_PART(metadata$filename, '/', 3)
   || '/' || SPLIT_PART(metadata$filename, '/', 4)
   || '/' || SPLIT_PART(metadata$filename, '/', 5), 'YYYY/MM/DD'),
 timestamp bigint AS (value:timestamp::bigint),
 col2 varchar AS (value:col2::varchar))
 PARTITION BY (date_part)
 LOCATION=@s1/logs/
 AUTO_REFRESH = true
 FILE_FORMAT = (TYPE = PARQUET)
 AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:001234567890:s3_mybucket';
 
 
 -- google cloud example 
CREATE STAGE s1
  URL='gcs://mybucket/files/logs/'
  ...
  ;
  
SELECT metadata$filename FROM @s1/;
 
CREATE EXTERNAL TABLE et1(
  date_part date AS TO_DATE(SPLIT_PART(metadata$filename, '/', 3)
    || '/' || SPLIT_PART(metadata$filename, '/', 4)
    || '/' || SPLIT_PART(metadata$filename, '/', 5), 'YYYY/MM/DD'),
  timestamp bigint AS (value:timestamp::bigint),
  col2 varchar AS (value:col2::varchar))
  PARTITION BY (date_part)
  LOCATION=@s1/logs/
  AUTO_REFRESH = true
  FILE_FORMAT = (TYPE = PARQUET);
  
  

-- Microsoft AZURE example
CREATE STAGE s1
  URL='azure://mycontainer/files/logs/'
  ...
  ;
  
SELECT metadata$filename FROM @s1/;
  
CREATE EXTERNAL TABLE et1(
  date_part date AS TO_DATE(SPLIT_PART(metadata$filename, '/', 3)
    || '/' || SPLIT_PART(metadata$filename, '/', 4)
    || '/' || SPLIT_PART(metadata$filename, '/', 5), 'YYYY/MM/DD'),
  timestamp bigint AS (value:timestamp::bigint),
  col2 varchar AS (value:col2::varchar))
  PARTITION BY (date_part)
  INTEGRATION = 'MY_INT'
  LOCATION=@s1/logs/
  AUTO_REFRESH = true
  FILE_FORMAT = (TYPE = PARQUET);


ALTER EXTERNAL TABLE et1 REFRESH;



-- External table created with detected column definitions
CREATE EXTERNAL TABLE mytable
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@mystage',
        FILE_FORMAT=>'my_parquet_format'
      )
    )
  )
  LOCATION=@mystage
  FILE_FORMAT=my_parquet_format
  AUTO_REFRESH=false;
  
  
  
  
-----------------------------------------:HYBRID TABLE:-----------------------------------------
/*
A hybrid table is a Snowflake table type that is optimized for hybrid transactional and operational workloads that require low latency and high throughput on small random point reads and writes. A hybrid table supports unique and referential integrity constraint enforcement that is critical for transactional workloads. You can use a hybrid table along with other Snowflake tables and features to power Unistore workloads that bring transactional and analytical data together in a single platform.

Use cases that may benefit from hybrid tables include:
	* Build a cohort for a targeted marketing campaign through an interactive user interface.
	* Maintain a central workflow state to coordinate large parallel data transformation pipelines.
	* Serve a precomputed promotion treatment for users who are visiting your website or mobile app.

-- Syntax
CREATE [ OR REPLACE ] HYBRID TABLE [ IF NOT EXISTS ] <table_name>
  ( <col_name> <col_type>
    [
      {
        DEFAULT <expr>
        | { AUTOINCREMENT | IDENTITY }
          [
            {
              ( <start_num> , <step_num> )
              | START <num> INCREMENT <num>
            }
          ]
          [ { ORDER | NOORDER } ]
      }
    ]
    [ NOT NULL ]
    [ inlineConstraint ]
    [ , <col_name> <col_type> [ ... ] ]
    [ , outoflineConstraint ]
    [ , outoflineIndex ]
    [ , ... ]
  )
  [ COMMENT = '<string_literal>' ]
*/

CREATE OR REPLACE HYBRID TABLE icecream (
  id NUMBER PRIMARY KEY AUTOINCREMENT START 1 INCREMENT 1,
  col1 VARCHAR NOT NULL,
  col2 VARCHAR NOT NULL
  );

INSERT INTO icecream VALUES(1, 'A1', 'B1');
INSERT INTO icecream VALUES(2, 'A2', 'B2');
INSERT INTO icecream VALUES(3, 'A3', 'B3');
INSERT INTO icecream VALUES(4, 'A4', 'B4');

UPDATE icecream SET col2 = 'B3-updated' WHERE id = 3;

DELETE FROM icecream WHERE id = 4;

SELECT * FROM icecream;





-----------------------------------------:ICEBERG TABLE:-----------------------------------------
/*
An Iceberg table uses the Apache Iceberg open table format specification, which provides an abstraction layer on data files stored in open formats and supports features such as:
	* ACID (atomicity, consistency, isolation, durability) transactions
	* Schema evolution
	* Hidden partitioning
	* Table snapshots

Iceberg tables for Snowflake combine the performance and query semantics of regular Snowflake tables with external cloud storage that you manage. They are ideal for existing data lakes that you cannot, or choose not to, store in Snowflake.

-- Syntax
CREATE [ OR REPLACE ] ICEBERG TABLE [ IF NOT EXISTS ] <table_name> (
    -- Column definition
    <col_name> <col_type>
      [ inlineConstraint ]
      [ NOT NULL ]
      [ COLLATE '<collation_specification>' ]
      [ { DEFAULT <expr>
          | { AUTOINCREMENT | IDENTITY }
            [ { ( <start_num> , <step_num> )
                | START <num> INCREMENT <num>
              } ]
        } ]
      [ [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ] ]
      [ [ WITH ] PROJECTION POLICY <policy_name> ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT '<string_literal>' ]

    -- Additional column definitions
    [ , <col_name> <col_type> [ ... ] ]

    -- Out-of-line constraints
    [ , outoflineConstraint [ ... ] ]
  )
  [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
  [ EXTERNAL_VOLUME = '<external_volume_name>' ]
  [ CATALOG = 'SNOWFLAKE' ]
  BASE_LOCATION = '<directory_for_table_files>'
  [ STORAGE_SERIALIZATION_POLICY = { COMPATIBLE | OPTIMIZED } ]
  [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
  [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
  [ CHANGE_TRACKING = { TRUE | FALSE } ]
  [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
  [ COPY GRANTS ]
  [ COMMENT = '<string_literal>' ]
  [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
  [ [ WITH ] AGGREGATION POLICY <policy_name> ]
  [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
*/

CREATE OR REPLACE ICEBERG TABLE iceberg_table_1 (
  col_1 int,
  col_2 string
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  BASE_LOCATION = 'iceberg_table_1';

CREATE OR REPLACE ICEBERG TABLE iceberg_table_2 (
  col_1 int,
  col_2 string
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  BASE_LOCATION = 'iceberg_table_2';