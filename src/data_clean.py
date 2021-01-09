import pyspark.sql.functions as F
from pprint import pprint 
from collections import defaultdict
import datetime

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
    
    for key, val in type_dict.items():
        if key in ["double","float"]:
            df = df.fillna(0,subset=val)    
        elif key == "string":
            df = df.fillna("Others", subset=["project_name","region","country"])
        elif key == "date":
            df = df.fillna(F.to_date(F.lit('1850/01/01'),'yyyy/MM/dd'), subset=["effective_date","closed_date","last_disbursed_date"])\
                    .withColumn("Days_to_Sign_the_loan",F.when(F.col("agreement_signing_date").isNull() | F.col("board_approval_date").isNull() ,\
                       F.lit(float("inf"))).otherwise(F.datediff("agreement_signing_date","board_approval_date")))\
                    .withColumn("Time_taken_for_Repayment",F.when(F.col("last_repayment_date").isNull() | F.col("last_repayment_date").isNull() ,\
                       F.lit(float("inf"))).otherwise(F.datediff("last_repayment_date","first_repayment_date")))\
                           .drop("last_repayment_date","first_repayment_date","agreement_signing_date","board_approval_date")
            
    return df

    

def preprocess(df):
    return type_dict(delete_columns(df))
