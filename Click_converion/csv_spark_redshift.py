from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Redshift credentials and connection parameters
redshift_host = 'spark-redshift-clus.cpf6dhezzpzr.ap-south-1.redshift.amazonaws.com'
redshift_port = '5439'
redshift_db = 'dev'
redshift_user = 'awsuser'
redshift_password = 'Tanay7614'

# CSV file path
csv_file_path = 'click_conversion_data.csv'

# Initialize SparkSession with Redshift JDBC driver
spark = SparkSession.builder \
    .appName("CSV to Redshift") \
    .config("spark.jars.packages","io.github.spark-redshift-community:spark-redshift_2.12:6.2.0-spark_3.4")\
    .config("spark.jars", "redshift-jdbc42-2.1.0.12.jar") \
    .getOrCreate()

# Read CSV file into DataFrame using pandas
df = spark.read.csv('click_conversion_data.csv', header=True, inferSchema=True)
print("Data read sucessfully")

df_spark = df.fillna('')

# Handling duplicates
df_spark = df_spark.dropDuplicates()

# Write Spark DataFrame to Redshift table
df.write \
    .format("jdbc") \
    .option("url", f"jdbc:redshift://{redshift_host}:{redshift_port}/{redshift_db}") \
    .option("dbtable", "click_conver_data") \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("overwrite") \
    .save()
print("Data write sucessfully")

# Stop SparkSession
spark.stop()
