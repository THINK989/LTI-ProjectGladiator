from pprint import pprint
import os


class SNOWFLAKELOANLOAD:
    def __init__(self):
        os.environ[
            "PYSPARK_SUBMIT_ARGS"
        ] = "--packages net.snowflake:snowflake-jdbc:3.8.0,net.snowflake:spark-snowflake_2.11:2.4.14-spark_2.4"
        self.sfOptions = {
            "sfURL": os.environ["sfURL"],
            "sfUser": os.environ["sfUser"],
            "sfPassword": os.environ["sfPassword"],
            "sfDatabase": "WORLDBANKLOAN_DB",
            "sfSchema": "LOAN_SCHEMA",
            "sfWarehouse": "PM_WORLDBANKLOAN",
        }
        self.SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    def snowflakeWrite(self, transformedDF):
        try:
            transformedDF.write.format(self.SNOWFLAKE_SOURCE_NAME).options(
                **self.sfOptions
            ).option("dbtable", "LOANSTATEMENT").save()

        finally:
            pass
        #     self.cs.close()
        # self.conn.close()

    def snowflakeRead(self, spark):
        df = (
            spark.read.format(self.SNOWFLAKE_SOURCE_NAME)
            .options(**self.sfOptions)
            .option("dbtable", "LOANSTATEMENT")
            .load()
        )
        df.printSchema()
        df.show()
