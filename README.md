# SCD-Type-2-Implementation-in-Snowflake

This Project aims to build SCD Type 2 functionality using Snowflake and Snowpark(Python)

A Slowly Changing Dimension (SCD) is a dimension that stores and manages both current and historical data over time in a data warehouse.
It is considered and implemented as one of the most critical ETL tasks in tracking the history of dimension records.

SCD type 2 stores the entire history of the data in the dimension table. With type 2 we can store unlimited history in the dimension table.
When the value of a chosen attribute changes, the current record is closed. A new record is created with the changed data values and this 
new record becomes the current record. Each record contains the effective time and expiration time to identify the time period between which the record was active.

The Files in this Project are to be read in the order:
  1. Table Creation 
  2. Improved_SCD_Type_1
  3. Executing the py script
  4. Automating the process
  5. Scheduling using Task
  
  
