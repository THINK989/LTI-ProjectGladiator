class BankLoan:

    def __init__(self, input_path=None):
        self.input_path = input_path

    # Read CSV file from source location
    def extract(self):
        # spark.read.options(header="true", inferSchema="true", delimiter=",").csv(self.input_path)
        return spark.sql("select * from worldbankloan_db.loanstatement")

    # Select Columns with transformations
    def transform(self, df):
        
        # How many days taken to sign the loan and the other was how many days taken for repayment.
        bsq1 = df.withColumn("Days_to_Sign_the_loan",F.datediff("agreement_signing_date","board_approval_date"))\
                .withColumn("Time_taken_for_Repayment",F.when(F.col("loan_status") == "Fully Repaid", F.datediff("last_repayment_date","first_repayment_date")).otherwise(F.lit(None)))
        
        # Find the  top three countries had huge amount of loans and it quickly dropped.
        bsq2 = df.groupby("country")\
                .agg(F.sum("orig_prin_amount").alias("Total_Amount"))\
                .orderBy(F.col("Total_Amount").desc())\
                .limit(3)
        
        # Which countries that had at least one cancellation with the assumption that they had borrowed enough number of loan
        set_ = set(df.select("country").where(F.col("loan_status") == "Fully Cancelled").rdd.flatMap(lambda x:x).collect())
        bsq3 = df.groupBy("country")\
                .agg(F.count("*").alias("Total_Requests"))\
                .where(F.col("country").isin(set_))\
                .orderBy(F.col("Total_Requests").desc())
        
        # Find the average repayment period for a country 
        bsq4 = bsq1.groupBy("country")\
                    .agg(F.round(F.avg("Time_taken_for_Repayment")).alias("Average_repayment_days"))\
                    .orderBy(F.col("country"))
        
        
        return (bsq1,bsq2,bsq3,bsq4)
    
    # Save the final Dataframe in your desired location in parquet/json format
    def load(self, bsq1,bsq2,bsq3,bsq4):
        # transformedDF.write\
        #     .bucketBy(4,"Country")\
        #     .saveAsTable("country_wise_loans", format="parquet")
        return bsq4.show(10)
        
    # Pipelining previous functions
    def run(self):
        self.load(*self.transform(self.extract()))


if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from data_clean import clean

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
    bk.run()
