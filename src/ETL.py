
class BankLoan:

    def __init__(self, input_path=None):        
        self.input_path = input_path

    # Read CSV file from source location
    def extract(self):
        
        # spark.read.options(header="true", inferSchema="true", delimiter=",").csv(self.input_path)
        return preprocess(spark.sql("select * from worldbankloan_db.loanstatement"))

    # Select Columns with transformations
    def transform(self, df):
        
        # How many days taken to sign the loan and the other was how many days taken for repayment.
        # bsq1 = df.withColumn("Days_to_Sign_the_loan",F.datediff("agreement_signing_date","board_approval_date"))\
        #         .withColumn("Time_taken_for_Repayment",F.when(F.col("loan_status") == "Fully Repaid", F.datediff("last_repayment_date","first_repayment_date")).otherwise(F.lit(None)))
        
        # Find the  top three countries had huge amount of loans and it quickly dropped.
        bsq2 = df.groupby("country")\
                .agg(F.sum("orig_prin_amount").alias("Total_Amount"))\
                .orderBy(F.col("Total_Amount").desc())\
                .limit(3)
        
        # Which countries that had at least one cancellation with the assumption that they had borrowed enough number of loan
        set_ = set(df.select("country").filter(F.col("loan_status").isin("Fully Cancelled","Cancelled")).rdd.flatMap(lambda x:x).collect())
        bsq3 = df.groupBy("country")\
                .agg(F.count("*").alias("Total_Requests"))\
                .where(F.col("country").isin(set_))\
                .orderBy(F.col("Total_Requests").desc())
        
        # Find the average repayment period for a country 
        bsq4 = df.groupBy("country")\
                    .agg(F.round(F.avg("time_taken_for_repayment")).alias("Average_repayment_days"))\
                    .orderBy(F.col("Average_repayment_days"))
        
        # top 10 loan type  ?
        bsq5 = df.groupBy("loan_type")\
                    .agg(F.count("*").alias("Number of Occurance"))\
                    .orderBy(F.col("Number of Occurance").desc())
        
        # In India , Which project type utilised more loans ?
        bsq6 = df.groupBy("project_name","country")\
                    .agg(F.count("*").alias("Number of Occurance"))\
                    .where(F.col("country")=="India")\
                    .orderBy(F.col("Number of Occurance").desc())
        
        # Percentage of loan issued to APAC region in current year ?
        total_amount = int(list(df.select(F.sum("orig_prin_amount")).rdd.flatMap(lambda x:x).collect())[0])
        bsq7 = df.groupBy("region")\
                    .agg(F.sum("orig_prin_amount").alias("Original Principle Amount"))\
                    .withColumn("Percentage of Loan", F.round((F.col("Original Principle Amount")*100)/total_amount,0))\
                    .orderBy(F.col("Percentage of Loan").desc())
        
        return (bsq2, bsq3, bsq4, bsq5, bsq6, bsq7, df)
    
    # Save the final Dataframe in your desired location in different codecs for parquet format
    def load(self, bsq2, bsq3, bsq4, bsq5, bsq6, bsq7, transformedDF):
        
        final_df = transformedDF.repartition('region')
        codecs = ["snappy","gzip","lz4","bzip2","deflate"]
        file_format = ["parquet", "orc"]
        # for fileformat in file_format:
        #     for cod in codecs:
        #         try:
        #             final_df.write\
        #                     .format(fileformat)\
        #                     .mode("overwrite")\
        #                     .options(codec=cod)\
        #                     .save("file:///"+str(Path.cwd())+"/data/output_region/df_"+fileformat+"_"+cod)
        #         except:
        #             pass
        #transformedDF.toPandas().to_csv('data/output.csv',index=False)
    
        s3Obj.run(transformedDF)
       
       # snowflakeObj.snowflakeWrite(transformedDF)
        snowflakeObj.snowflakeRead(spark)
    
    
    # Pipelining previous functions
    def run(self):
        self.load(*self.transform(self.extract()))


if __name__ == "__main__":
    import findspark
    findspark.init()
    import argparse
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pathlib import Path
    from data_clean import preprocess
    from spark_s3 import S3LOANLOAD
    from spark_snowflake import SNOWFLAKELOANLOAD

    # Used to make it work with python directly without involving spaprk-submit
    spark = SparkSession.builder.master('local')\
                                .appName('Bank_Loan')\
                                .enableHiveSupport()\
                                .getOrCreate()                         
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path',
                        type=str,
                        default='data/IBRD_Statement_Of_Loans_-_Historical_Data.csv')
    args = parser.parse_args()
    bk = BankLoan(input_path=args.input_path)
    s3Obj=S3LOANLOAD()
    snowflakeObj=SNOWFLAKELOANLOAD()
    bk.run()
    
    