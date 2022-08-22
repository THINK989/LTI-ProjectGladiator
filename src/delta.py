class DeltaLoans:
    def __init__(self, input_path=None, file_to_merge=None):
        self.input_path = input_path
        self.file_to_merge = file_to_merge

    def extract(self):
        table_schema = spark.sql("select * from worldbankloan_db.loanstatement").schema
        return preprocess(
            spark.read.schema(table_schema)
            .options(header="false", delimiter=",")
            .csv(self.file_to_merge)
        )

    def load(self, df):
        if not os.path.exists("output_region/df_csv"):
            df.repartition("region").write.mode("overwrite").options(header="true", inferSchema="true", delimiter=",").csv(f"file:///{str(Path(__file__).parent)}/output_region/df_csv")

    def delta(self):
        self.load(self.extract())
        if not os.path.exists("delta"):
            for file in os.listdir(self.input_path):
                filetype = file.split("_")[1]
                if filetype == "orc":
                    spark.read.orc(self.input_path + file).write.format("delta").save(f"{str(Path(__file__).parent)}/delta/delta{file}")

                elif filetype == "parquet":
                    spark.read.parquet(self.input_path + file).write.format("delta").save(f"{str(Path(__file__).parent)}/delta/delta{file}")

                else:
                    spark.read.options(header="true", inferSchema="true", sep=",").csv(self.input_path + file).write.format("delta").save(f"{str(Path(__file__).parent)}/delta/delta{file}")

        deltaTable_history = DeltaTable.forPath(spark, "delta/deltadf_parquet_snappy")
        deltaTable_latest = DeltaTable.forPath(spark, "delta/deltadf_csv")
        deltaTable_history.alias("history").merge(deltaTable_latest.toDF().alias("updates"), "history.end_of_period = updates.end_of_period").whenNotMatchedInsertAll().execute()

        deltaTable_history.toDF().toPandas().to_csv("data/output.csv", index=False)

    # run ETL function
    def run(self):
        self.delta()


# 996342
# 996332
if __name__ == "__main__":
    import argparse, os
    import findspark

    findspark.init()
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pathlib import Path
    from data_clean import preprocess

    spark = (
        SparkSession.builder.appName("Bank_Loan")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    from delta.tables import *

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, default="output_region/")
    parser.add_argument(
        "--file_to_merge",
        type=str,
        default="data/IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv",
    )
    args = parser.parse_args()
    bk = DeltaLoans(input_path=args.input_path, file_to_merge=args.file_to_merge)
    bk.run()
