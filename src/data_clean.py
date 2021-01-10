import pyspark.sql.functions as F
from pprint import pprint 
from collections import defaultdict

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
            
    return df

def string_handling(df):
    return df.withColumn("region_upper", F.upper(F.col("region"))).drop("region")\
            .withColumnRenamed("region_upper","region")
    

def preprocess(df):
    return string_handling(type_dict(df))
