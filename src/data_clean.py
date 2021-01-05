import pyspark.sql.functions as F

def delete_columns(df):
    count_ = df.count()
    dict_ = {column:df.filter(df[column].isNull()).count() for column in df.columns}
    columns_to_delete = [key for key, value in dict_.items() if value > (count_//2)]
    return df.drop(*columns_to_delete)


def clean(df):
    return delete_columns(df)
