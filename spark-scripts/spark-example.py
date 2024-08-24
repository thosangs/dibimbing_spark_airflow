import pyspark

postgres_host = "dataeng-postgres"
postgres_dw_db = "warehouse"
postgres_user = "user"
postgres_password = "password"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(pyspark.SparkConf().setAppName("Dibimbing")))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f"jdbc:postgresql://{postgres_host}/{postgres_dw_db}"
jdbc_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}
print(jdbc_properties)
retail_df = spark.read.jdbc(jdbc_url, "public.retail", properties=jdbc_properties)

retail_df.show(5)
