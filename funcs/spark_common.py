host = "localhost"
port = 5432
user = "<USER>>"
password = "<PASSWORD>"

def spark_read(spark, dbtable):
    data = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{host}:{port}/movies") \
    .option("dbtable", dbtable) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
    return data