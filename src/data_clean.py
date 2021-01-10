import pyspark.sql.functions as F
from pprint import pprint 
from collections import defaultdict

import datetime
import csv




import datetime
import csv



def delete_columns(df):
    count_ = df.count()
    dict_ = {column:df.filter(df[column].isNull()).count() for column in df.columns}
    columns_to_delete = [key for key, value in dict_.items() if value > (count_//2)]
    pprint(dict_)
    return df.drop(*columns_to_delete)

def type_dict(df):
    type_dict = defaultdict(list)
    for col_name,type in df.dtypes:
        type_dict[type].append(col_name)
    print(type_dict)
    
    for key, val in type_dict.items():
        if key in ["double","float"]:
            df = df.fillna(0,subset=val)    
        elif key == "string":
            df = df.fillna("Others", subset=["project_name","region","country","borrower","project_id"])\
                    .na.drop(subset=["loan_status","loan_type"])
        elif key == "date":
            for col_name in ["effective_date","closed_date","last_disbursed_date"]:
                df = df.withColumn(col_name+"_clone",\
                        F.when(F.col(col_name).isNull(),\
                        F.to_date(F.lit('1850/01/01'),'yyyy/MM/dd'))\
                        .otherwise(F.col(col_name)))\
                        .drop(col_name)\
                        .withColumnRenamed(col_name+"_clone",col_name)
                   
            df =  df.withColumn("Days_to_Sign_the_loan",F.when(F.col("agreement_signing_date").isNull() | F.col("board_approval_date").isNull() ,\
                        F.lit(float("inf"))).otherwise(F.datediff("agreement_signing_date","board_approval_date")))\
                    .withColumn("Time_taken_for_Repayment",F.when(F.col("last_repayment_date").isNull() | F.col("last_repayment_date").isNull() ,\
                        F.lit(float("inf"))).otherwise(F.datediff("last_repayment_date","first_repayment_date")))\
                            .drop("last_repayment_date","first_repayment_date","agreement_signing_date","board_approval_date")
            
    countryCode=df.select('country_code','country').rdd.collect()
    # country=df.select('country').rdd.flatMap(lambda x: x).collect()
    countryList=list(set(list(countryCode)))
    # for i in countryList:
    #     print(i['country_code'],i['country'])

    # with open('data/country.csv', 'w', newline='') as file:

    # with open('country.csv', 'w', newline='') as file:

    #     writer = csv.writer(file)
    #     writer.writerow(["country_code", "country"])
    #     for i in countryList:
    #         writer.writerow([str(i['country_code']),str(i['country'])])
    
    return df

    

def preprocess(df):
    return type_dict(df)
