from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("cb_etl")
    .config("spark.driver.extraClassPath", "drivers/postgresql-42.3.4.jar")
    .getOrCreate()
)

df = spark.read.parquet("datasets/final.parquet")

dbconfig = {
    "host": "localhost",
    "port": "5432",
    "database": "prefect_server",
    "user": "prefect",
    "password": "test-password",
}

url = (
    "jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}".format(
        **dbconfig
    )
)

df.write.format("jdbc").option(
    "url", "jdbc:postgresql://localhost:5432/prefect_server"
).option("truncate", True).option("dbtable", "public.etl_data").option(
    "user", dbconfig["user"]
).option(
    "password", dbconfig["password"]
).save()
