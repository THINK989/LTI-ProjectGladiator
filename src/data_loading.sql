create database WorldBankLoan_DB;
use WorldBankLoan_DB;


--Create Table Mysql
Create table loanStatement (
    end_of_period date,
    loan_number varchar(255),
    region varchar(255),
    country_code varchar(255),
    country varchar(255),
    borrower varchar(255),
    gaurantor_countrycode varchar(255),
    gaurantor_country varchar(255),
    loan_type varchar(255),
    loan_status varchar(255),
    interest_rate float(4),
    curr_of_commitment varchar(255),
    project_id varchar(255),
    project_name varchar(255),
    orig_prin_amount double,
    cancelled_amount double,
    undisbursed_amount double,
    disbursed_amount double,
    repaid_to_IBRD double,
    due_to_ibrd double,
    exchange_adjustment double,
    borrowers_obligation double,
    sold_3rd_party double,
    repaid_3rd_party double,
    due_3rd_party double,
    loans_held double,
    first_repayment_date date,
    last_repayment_date date,
    agreement_signing_date date,
    board_approval_date date,
    effective_date date,
    closed_date date,
    last_disbursed_date date
);

--Load data from csv file
load data infile '/var/lib/mysql-files/IBRD_Statement_Of_Loans_-_Historical_Data.csv' 
into table loanStatement 
fields terminated by ',' 
enclosed by '"' 
lines terminated by '\n' 
ignore 1 rows 
(
    @end_of_period,
    loan_number,
    region,
    country_code,
    country,
    borrower,
    gaurantor_countrycode,
    gaurantor_country,
    loan_type,
    loan_status,
    interest_rate,
    curr_of_commitment,
    project_id,
    project_name,
    orig_prin_amount,
    cancelled_amount,
    undisbursed_amount,
    disbursed_amount,
    repaid_to_IBRD,
    due_to_ibrd,
    exchange_adjustment,
    borrowers_obligation,
    sold_3rd_party,
    repaid_3rd_party,
    due_3rd_party,
    loans_held,
    @first_repayment_date,
    @last_repayment_date,
    @agreement_signing_date,
    @board_approval_date,
    @effective_date,
    @closed_date,
    @last_disbursed_date
)
set end_of_period = str_to_date(@end_of_period, '%m/%d/%Y'),
    first_repayment_date = str_to_date(@first_repayment_date, '%m/%d/%Y'),
    last_repayment_date = str_to_date(@last_repayment_date, '%m/%d/%Y'),
    agreement_signing_date = str_to_date(@agreement_signing_date, '%m/%d/%Y'),
    board_approval_date = str_to_date(@board_approval_date, '%m/%d/%Y'),
    effective_date = str_to_date(@effective_date, '%m/%d/%Y'),
    closed_date = str_to_date(@closed_date, '%m/%d/%Y'),
    last_disbursed_date = str_to_date(@last_disbursed_date, '%m/%d/%Y');

SELECT * FROM loanStatement;

SHOW VARIABLES LIKE "secure_file_priv";

--Allow NULL values
SET @@SESSION.sql_mode='ALLOW_INVALID_DATES';

-- Create Hive Tables
Create external table if not exists loanStatement (
    end_of_period date,
    loan_number string,
    region string,
    country_code string,
    country string,
    borrower string,
    gaurantor_countrycode string,
    gaurantor_country string,
    loan_type string,
    loan_status string,
    interest_rate float,
    curr_of_commitment string,
    project_id string,
    project_name string,
    orig_prin_amount double,
    cancelled_amount double,
    undisbursed_amount double,
    disbursed_amount double,
    repaid_to_IBRD double,
    due_to_ibrd double,
    exchange_adjustment double,
    borrowers_obligation double,
    sold_3rd_party double,
    repaid_3rd_party double,
    due_3rd_party double,
    loans_held double,
    first_repayment_date date,
    last_repayment_date date,
    agreement_signing_date date,
    board_approval_date date,
    effective_date date,
    closed_date date,
    last_disbursed_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/root/hiveEtxTable/WorldBankLoan';

hive> select count(*) from loanStatement;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = user_20210111172454_7c8613a5-4528-4bb8-a467-0add7b17e5c0
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1610298422803_0001, Tracking URL = http://flx:8088/proxy/application_1610298422803_0001/
Kill Command = /home/user/hadoop-2.10.1/bin/hadoop job  -kill job_1610298422803_0001
Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 1
2021-01-11 17:25:41,470 Stage-1 map = 0%,  reduce = 0%
2021-01-11 17:26:02,377 Stage-1 map = 50%,  reduce = 0%, Cumulative CPU 3.97 sec
2021-01-11 17:26:05,658 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 10.99 sec
2021-01-11 17:26:14,324 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 14.19 sec
MapReduce Total cumulative CPU time: 14 seconds 190 msec
Ended Job = job_1610298422803_0001
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 2  Reduce: 1   Cumulative CPU: 14.19 sec   HDFS Read: 289134370 HDFS Write: 106 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 190 msec
OK
987441
Time taken: 83.077 seconds, Fetched: 1 row(s)
    