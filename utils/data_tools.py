import json
from pandas.io.json._normalize import nested_to_record
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def read_csv(filepath, session):
    return session.read.csv(filepath, quote='"', escape='"', header=True)


def flatten(dictionary):
    return nested_to_record(dictionary, sep="_")


def dict_parser(instance):
    out_instance = {}
    for k, v in instance.items():
        try:
            v = json.loads(v)
            if isinstance(v, dict):
                out_instance[k] = dict_parser(v)
            out_instance[k] = v
        except (json.JSONDecodeError, TypeError):
            out_instance[k] = v
    return out_instance


def transform(row, transformations=(Row.asDict, dict_parser, flatten)):
    for transformation in transformations:
        try:
            row = transformation(row)
        except:
            print(row)
    return row


def cast_columns(df, column_cast_type_map: dict):
    for col_name, cast_type in column_cast_type_map.items():
        df = DataFrame.withColumn(df, col_name, col(col_name).cast(cast_type))
    return df
