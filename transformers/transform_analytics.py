from pyspark.sql import functions
from pyspark.sql import DataFrame


def transform(df_analytics):
    user_info = (
        df_analytics.groupby("user_id")
        .agg(
            {
                "device_language": "first",
                "city_geoIp": "first",
                "app_version": "first",
                "device_model": "first",
            }
        )
        .withColumnRenamed("first(app_version)", "app_version")
        .withColumnRenamed("first(device_model)", "phone_model")
        .withColumnRenamed("first(city_geoIp)", "city")
        .withColumnRenamed("first(device_language)", "language")
    )

    return user_info
