--Create Table Mysql
-- Create Historical table
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

--Load  Historical data from csv file
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


SHOW VARIABLES LIKE "secure_file_priv";

-- Create latest table
Create table loanStatement_latest (
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
load data infile '/var/lib/mysql-files/IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv' 
into table loanStatement_latest
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
set end_of_period = str_to_date(@end_of_period, "%m/%d/%Y"),
    first_repayment_date = str_to_date(@first_repayment_date, "%m/%d/%Y"),
    last_repayment_date = str_to_date(@last_repayment_date, "%m/%d/%Y"),
    agreement_signing_date = str_to_date(@agreement_signing_date, "%m/%d/%Y"),
    board_approval_date = str_to_date(@board_approval_date, "%m/%d/%Y"),
    effective_date = str_to_date(@effective_date, "%m/%d/%Y"),
    closed_date = str_to_date(@closed_date, "%m/%d/%Y"),
    last_disbursed_date = str_to_date(@last_disbursed_date, "%m/%d/%Y");


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


    