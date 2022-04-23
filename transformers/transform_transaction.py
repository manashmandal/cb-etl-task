from pyspark.sql import functions
from pyspark.sql import DataFrame


def transform(df_transaction):
    amount_of_credits = (
        df_transaction.select("data_user_id", "data_amount", "data_transaction_type")
        .filter(df_transaction.data_transaction_type == "credit")
        .groupby("data_user_id")
        .sum("data_amount")
        .withColumnRenamed("sum(data_amount)", "amount_of_credits")
        .withColumnRenamed("data_user_id", "user_id")
    )

    amount_of_debits = (
        df_transaction.select("data_user_id", "data_amount", "data_transaction_type")
        .filter(df_transaction.data_transaction_type == "debit")
        .groupby("data_user_id")
        .sum("data_amount")
        .withColumnRenamed("sum(data_amount)", "amount_of_debits")
        .withColumnRenamed("data_user_id", "user_id")
    )

    no_of_credits = (
        df_transaction.select("data_user_id", "data_amount", "data_transaction_type")
        .filter(df_transaction.data_transaction_type == "credit")
        .groupby("data_user_id")
        .count()
        .withColumnRenamed("count", "no_of_credits")
        .withColumnRenamed("data_user_id", "user_id")
    )

    no_of_debits = (
        df_transaction.select("data_user_id", "data_amount", "data_transaction_type")
        .filter(df_transaction.data_transaction_type == "debit")
        .groupby("data_user_id")
        .count()
        .withColumnRenamed("count", "no_of_debits")
        .withColumnRenamed("data_user_id", "user_id")
    )

    amount_of_total_transactions = (
        df_transaction.select("data_user_id", "data_amount", "data_transaction_type")
        .groupby("data_user_id")
        .sum("data_amount")
        .withColumnRenamed("sum(data_amount)", "amount_of_total_transactions")
        .withColumnRenamed("data_user_id", "user_id")
    )

    no_of_transactions = (
        df_transaction.select("data_user_id", "data_amount", "data_transaction_type")
        .groupby("data_user_id")
        .count()
        .withColumnRenamed("count", "no_of_transactions")
        .withColumnRenamed("data_user_id", "user_id")
    )

    calculated_fields_median_gmv_per_month = (
        df_transaction.groupby("data_user_id", functions.month("timestamp"))
        .agg(functions.sum("data_amount").alias("amount"))
        .groupby("data_user_id")
        .agg(
            functions.percentile_approx("amount", 0.5).alias(
                "calculated_fields.median_gmv_per_month"
            )
        )
        .withColumnRenamed("data_user_id", "user_id")
    )

    calculated_field_median_trans_per_month = (
        df_transaction.groupby("data_user_id", functions.month("timestamp"))
        .agg(functions.count("data_amount").alias("amount"))
        .groupby("data_user_id")
        .agg(
            functions.percentile_approx("amount", 0.5).alias(
                "calculated_fields.median_trans_per_month"
            )
        )
        .withColumnRenamed("data_user_id", "user_id")
    )

    calculated_field_months_transacting = (
        df_transaction.select(
            "data_user_id", functions.month("timestamp").alias("month_number")
        )
        .groupby("data_user_id")
        .agg(
            functions.expr("count(month_number)").alias(
                "calculated_fields.months_transacting"
            )
        )
        .withColumnRenamed("data_user_id", "user_id")
    )

    out = amount_of_credits

    for df in [
        amount_of_debits,
        no_of_credits,
        no_of_debits,
        amount_of_total_transactions,
        no_of_transactions,
        calculated_fields_median_gmv_per_month,
        calculated_field_median_trans_per_month,
        calculated_field_months_transacting,
    ]:
        out = DataFrame.join(out, df, how="inner", on="user_id")

    return out
