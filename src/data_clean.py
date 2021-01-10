from pyspark.sql import functions as F
from pprint import pprint 
from collections import defaultdict
import csv, os


def delete_columns(df):
    count_ = df.count()
    dict_ = {column:df.filter(df[column].isNull()).count() for column in df.columns}
    columns_to_delete = [key for key, value in dict_.items() if value > (count_//2)]
    print(dict_)
    return df.drop(*columns_to_delete)

def country_code_hashmap(df):
    #TODO: create a csv file containing unique country_code and country
    if not os.path.exists("country.csv"):
        countryCode=set(df.select('country_code','country').rdd.collect())
        with open('country.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["country_code", "country"])
            for country_code,country in countryCode:
                writer.writerow([country_code,country])
    return df
       

def handling_null(df):
    #TODO: create a dictionary as {key(datatype):list(value) of column names} and handle each type separately
    type_dict = defaultdict(list)
    for col_name,type in df.dtypes:
        type_dict[type].append(col_name)
    
    for key, val in type_dict.items():
        #* If data is of type double, float replace nulls with 0
        if key in ["double","float"]:
            df = df.fillna(0,subset=val) 
        
        #* If data is of type string    
        elif key == "string":
            #* fill null values with "Other"
            df = df.fillna("Other", subset=["project_name","region","country","borrower","project_id"])\
                    .na.drop(subset=["loan_status","loan_type"])
            
            #* use country code and country as hashmap to replace null values and drop rows where it is not possible        
            with open("country.csv", mode='r') as country_code:
                code_reader = csv.reader(country_code)
                for country_code, country in code_reader:
                    df = df.withColumn("gaurantor_country_clone", F.when(F.col("gaurantor_countrycode")==country_code, country).otherwise(F.col("gaurantor_country")))\
                            .withColumn("gaurantor_countrycode_clone", F.when(F.col("gaurantor_country")==country, country_code).otherwise(F.col("gaurantor_countrycode")))
                            
            for col_name in ["gaurantor_countrycode","gaurantor_country"]:
                    df = df.drop(col_name)\
                            .withColumnRenamed(col_name+"_clone",col_name)\
                            .na.drop(col_name)
        
        #* If data is of type date         
        elif key == "date":
            
            #* Replace NULL values with an outlier date
            for col_name in ["effective_date","closed_date","last_disbursed_date","end_of_period"]:
                df = df.withColumn(col_name+"_clone",\
                        F.when(F.col(col_name).isNull(),\
                        F.to_date(F.lit('1850/01/01'),'yyyy/MM/dd'))\
                        .otherwise(F.col(col_name)))\
                        .drop(col_name)\
                        .withColumnRenamed(col_name+"_clone",col_name)
            
            #* Replace columns with a more informative column       
            df =  df.withColumn("days_to_sign_the_loan",F.when(F.col("agreement_signing_date").isNull() | F.col("board_approval_date").isNull() ,\
                        F.lit(float("inf"))).otherwise(F.datediff("agreement_signing_date","board_approval_date")))\
                    .withColumn("time_taken_for_repayment",F.when(F.col("last_repayment_date").isNull() | F.col("first_repayment_date").isNull() ,\
                        F.lit(float("inf"))).otherwise(F.datediff("last_repayment_date","first_repayment_date")))\
                            .drop("last_repayment_date","first_repayment_date","agreement_signing_date","board_approval_date")
      
    return df

def string_handling(df):
    return df.withColumn("region_upper", F.upper(F.col("region"))).drop("region")\
            .withColumnRenamed("region_upper","region")
    

def preprocess(df):
    return delete_columns(handling_null(df))
