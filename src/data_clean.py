import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from re import sub

def delete_columns(df):
    count_ = df.count()
    dict_ = {column:df.filter(df[column].isNull()).count() for column in df.columns}
    columns_to_delete = [key for key, value in dict_.items() if value > (count_//2)]
    return df.drop(*columns_to_delete)


def renaming(df):
    return df.select([F.col(column).alias(sub(r"\([^)]*\)","",column).strip().replace(" ","_").lower()) for column in df.columns])
    
def clean(df):
    return renaming(df)

def date_format(date_column):
    return date_column.split()[0]

