from pyspark.sql import functions
from pyspark.sql import DataFrame


def transform(df_user):
    rating = (
        df_user.select("user_id", "data_rating_stars")
        .groupby("user_id")
        .avg("data_rating_stars")
        .withColumnRenamed("avg(data_rating_stars)", "rating")
    )

    user_last_activity = (
        df_user.select("user_id", "data_user_last_activity__seconds")
        .groupby("user_id")
        .agg({"data_user_last_activity__seconds": "max"})
        .withColumnRenamed(
            "max(data_user_last_activity__seconds)", "user_last_activity"
        )
    )

    user_last_activity = user_last_activity.withColumn(
        "user_last_activity", functions.col("user_last_activity").cast("timestamp")
    )

    days_since_last_activity = user_last_activity.withColumn(
        "user_last_activity",
        functions.datediff(
            functions.current_date(), functions.col("user_last_activity")
        ),
    ).withColumnRenamed("user_last_activity", "days_since_last_activity")

    created_at = (
        df_user.select("user_id")
        .dropDuplicates(["user_id"])
        .withColumn("created_at", functions.current_timestamp())
    )

    days_since_signup = (
        df_user.select("user_id", "data_user_signup_date__seconds")
        .groupBy("user_id")
        .agg({"data_user_signup_date__seconds": "min"})
        .withColumnRenamed("min(data_user_signup_date__seconds)", "signup_timestamp")
        .withColumn(
            "signup_timestamp",
            functions.col("signup_timestamp").cast("timestamp").cast("date"),
        )
        .withColumn(
            "days_since_signup",
            functions.datediff(
                functions.current_date(), functions.col("signup_timestamp")
            ),
        )
        .select("user_id", "days_since_signup")
    )

    out = rating
    for df in [
        user_last_activity,
        days_since_last_activity,
        days_since_signup,
        created_at,
    ]:
        out = DataFrame.join(out, df, how="inner", on="user_id")
    return out
