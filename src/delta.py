class DeltaLoans:

    def __init__(self, input_path=None):
        self.input_path = input_path

    def ETL(self):
        for file in os.listdir(self.input_path):
            filetype = file.split("_")[1]
            if filetype == "orc":
                spark.read.orc(self.input_path+file)\
                        .write.format("delta")\
                        .save(str(Path(__file__).parent)+"/delta/delta"+file)
            else:
                spark.read.parquet(self.input_path+file)\
                        .write.format("delta")\
                        .save(str(Path(__file__).parent)+"/delta/delta"+file)
        
    # run ETL function
    def run(self):
        self.ETL()


if __name__ == "__main__":
    import argparse, os
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pathlib import Path
    
    # Used to make it work with python directly without involving spaprk-submit
    spark = SparkSession.builder\
                        .appName('Bank_Loan')\
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                        .getOrCreate()                         
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path',
                        type=str,
                        default='output/')
    args = parser.parse_args()
    bk = DeltaLoans(input_path=args.input_path)
    bk.run()
