import pyspark.sql.functions as F


def fill_null(df):
    return df.fillna("Others",subset=["project_name"])


def string_handling(df):
    return df.withColumn("region_upper", F.upper(F.col("region"))).drop("region")\
            .withColumnRenamed("region_upper","region")


def preprocess(df):
    return string_handling(fill_null(df))
