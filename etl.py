from prefect import Flow, task, resource_manager, Client
import os
from pyspark.sql import SparkSession, types
from utils import data_tools
import shutil
from transformers import transform_user, transform_analytics, transform_transaction
from dotenv import load_dotenv

load_dotenv()

c = Client(api_server="http://graphql:4200")
try:
    c.create_tenant("CB_ETL", "cb-etl")
    c.create_project("CB_ETL")
except:
    pass


subpath = "./datasets"
relative_path = lambda filepath: os.path.join(subpath, filepath)
transaction_dataset = relative_path("transactions.csv")
users_dataset = relative_path("users.csv")
analytics_dataset = relative_path("analytics.csv")

dbconfig = {
    "host": os.environ.get("WRITE_DB_HOST", "postgres"),
    "port": os.environ.get("WRITE_DB_PORT", "5432"),
    "database": os.environ.get("WRITE_DB_DB", "prefect_server"),
    "user": os.environ.get("WRITE_DB_USER", "prefect"),
    "password": os.environ.get("WRITE_DB_PASSWORD", "test-password"),
}


@resource_manager
class SparkCluster:
    def setup(self) -> SparkSession:
        return (
            SparkSession.builder.appName("Creditbook ETL")
            .config("spark.driver.extraClassPath", "drivers/postgresql-42.3.4.jar")
            .getOrCreate()
        )

    def cleanup(self, spark: SparkSession):
        spark.stop()


@task
def extract(session):
    df_transaction = data_tools.read_csv(transaction_dataset, session)
    df_user = data_tools.read_csv(users_dataset, session)
    df_analytics = data_tools.read_csv(analytics_dataset, session)

    df_transaction = df_transaction.rdd.map(data_tools.transform).toDF()
    df_user = df_user.rdd.map(data_tools.transform).toDF(sampleRatio=0.4)
    df_analytics = df_analytics.rdd.map(data_tools.transform).toDF()

    df_transaction = data_tools.cast_columns(
        df_transaction,
        {
            "data_amount": types.FloatType(),
            "timestamp": types.TimestampType(),
        },
    )

    df_user = data_tools.cast_columns(
        df_user,
        {
            "timestamp": types.TimestampType(),
        },
    )
    df_user = df_user.withColumnRenamed("data_id", "user_id").drop(
        "data_location_our_logs",
        "data_location_past_logs",
        "data_user_signup_date",
        "data_rating_rated_timestamp",
        "data_user_last_activity",
    )

    df_user_path = relative_path("user.parquet")
    df_transaction_path = relative_path("transaction.parquet")
    df_analytics_path = relative_path("analytics.parquet")

    shutil.rmtree(df_user_path, ignore_errors=True)
    shutil.rmtree(df_transaction_path, ignore_errors=True)
    shutil.rmtree(df_analytics_path, ignore_errors=True)

    df_user.write.parquet(df_user_path)
    df_transaction.write.parquet(df_transaction_path)
    df_analytics.write.parquet(df_analytics_path)

    return {
        "transaction": df_transaction_path,
        "user": df_user_path,
        "analytics": df_analytics_path,
    }


@task
def transform(extracted_data_paths, session):
    df_transaction = session.read.parquet(extracted_data_paths["transaction"])
    df_user = session.read.parquet(extracted_data_paths["user"])
    df_analytics = session.read.parquet(extracted_data_paths["analytics"])

    out_df_user = transform_user.transform(df_user)
    out_df_transaction = transform_transaction.transform(df_transaction)
    out_df_analytics = transform_analytics.transform(df_analytics)

    final = out_df_user.join(out_df_transaction, on="user_id", how="inner").join(
        out_df_analytics, on="user_id", how="inner"
    )

    final_path = relative_path("final.parquet")
    shutil.rmtree(final_path, ignore_errors=True)
    final.write.parquet(final_path)

    return {"final": final_path}


@task
def load(transformed_data_paths, session):
    final_df = session.read.parquet(transformed_data_paths["final"])
    final_df.write.format("jdbc").option(
        "url",
        f"jdbc:postgresql://{dbconfig['host']}:{dbconfig['port']}/{dbconfig['database']}",
    ).option("truncate", True).option("dbtable", "public.etl_data").option(
        "user", dbconfig["user"]
    ).option(
        "password", dbconfig["password"]
    ).save()


with Flow("CB_ETL") as flow:
    with SparkCluster() as spark:
        extracted_data_paths = extract(spark)
        transformed_data_paths = transform(extracted_data_paths, spark)
        load(transformed_data_paths, spark)


if __name__ == "__main__":
    flow.register(
        project_name="CB_ETL",
    )
    flow.run_agent(api_key=c.api_key)
