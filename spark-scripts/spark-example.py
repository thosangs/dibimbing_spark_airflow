import pyspark

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("Dibimbing"))
)
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# define schema for purchases dataset
purchases_schema = (
    "order_id int, customer_id int, product_id int, quantity int, price float"
)

# create purchases dataframe
purchases_data = [
    (101, 1, 1, 2, 19.99),
    (102, 2, 2, 1, 9.99),
    (103, 3, 3, 1, 15.99),
    (104, 1, 4, 1, 5.99),
    (105, 2, 5, 3, 12.99),
    (106, 3, 6, 2, 9.99),
    (107, 4, 7, 1, 11.99),
    (108, 1, 8, 2, 14.99),
    (109, 2, 9, 1, 9.99),
    (110, 3, 10, 1, 19.99),
]
purchases_df = spark.createDataFrame(purchases_data, schema=purchases_schema)

# define schema for customers dataset
customers_schema = "customer_id int, name string, email string"

# create customers dataframe
customers_data = [
    (1, "John Doe", "johndoe@example.com"),
    (2, "Jane Smith", "janesmith@example.com"),
    (3, "Bob Johnson", "bobjohnson@example.com"),
    (4, "Sue Lee", "suelee@example.com"),
]
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# define schema for products dataset
products_schema = "product_id int, name string, price float"

# create products dataframe
products_data = [
    (1, "Product A", 19.99),
    (2, "Product B", 9.99),
    (3, "Product C", 15.99),
    (4, "Product D", 5.99),
    (5, "Product E", 12.99),
    (6, "Product F", 9.99),
    (7, "Product G", 11.99),
    (8, "Product H", 14.99),
    (9, "Product I", 9.99),
    (10, "Product J", 19.99),
]
products_df = spark.createDataFrame(products_data, schema=products_schema)

# set join preferences
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# perform sort merge join
merged_df = purchases_df.join(customers_df, "customer_id").join(
    products_df, "product_id"
)
merged_df.show()
