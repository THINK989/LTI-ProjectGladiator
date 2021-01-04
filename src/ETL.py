class BankLoan:

    def __init__(self, input_path=None):
        self.input_path = input_path
        self.spark_session = (SparkSession.builder
                              .master("local[1]")
                              .appName("Bank_Loan")
                              .getOrCreate())

    # Read CSV file from source location
    def extract(self):
        return clean(spark.read.options(header="true", inferSchema="true", delimiter=",").csv(self.input_path))

    # Select Columns with transformations
    def transform(self, df):
        return df.withColumn("Days_to_Sign_the_loan",F.datediff(date_format("effective_date"),date_format("agreement_signing_date")))

    # Save the final Dataframe in your desired location in parquet/json format
    def load(self, transformedDF):
        # transformedDF.write\
        #     .bucketBy(4,"Country")\
        #     .saveAsTable("country_wise_loans", format="parquet")
        return transformedDF.show()
        
    # Pipelining previous functions
    def run(self):
        self.load(self.transform(self.extract()))


if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession
    from pyspark.context import SparkContext
    import pyspark.sql.functions as F
    from data_clean import clean,date_format

    # Used to make it work with python directly without involving spaprk-submit
    sc = SparkContext('local')
    spark = SparkSession(sc)
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path',
                        type=str,
                        default='data/IBRD_Statement_Of_Loans_-_Historical_Data.csv')
    args = parser.parse_args()
    bk = BankLoan(input_path=args.input_path)
    bk.run()
