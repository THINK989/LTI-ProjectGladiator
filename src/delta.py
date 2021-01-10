
class DeltaLoans:

    def __init__(self, input_path=None):
        self.input_path = input_path

    # Read CSV file from source location
    def extract(self):
        return spark.read.orc(self.input_path+"df_orc_lz4")
        
    
    # Select Columns with transformations
    def transform(self, df):
        return df
    
    # Save the final Dataframe in your desired location in different codecs for parquet format
    def load(self, transformedDF):
        transformedDF.write.format("delta").save(str(Path(__file__).parent)+"/delta/df_orc_delta")

        
    # Pipelining previous functions
    def run(self):
        self.load(self.transform(self.extract()))


if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pathlib import Path
    from data_clean import preprocess

    # Used to make it work with python directly without involving spaprk-submit
    spark = SparkSession.builder\
                        .appName('Bank_Loan')\
                        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0.jar")\
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
