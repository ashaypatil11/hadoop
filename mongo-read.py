from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

spark = SparkSession.\
    builder.appName("mongo-spark").\
        getOrCreate()

readUsers = spark.read.\
    format("com.mongodb.spark.sql.DefaultSource").\
        option("uri", "mongodb://127.0.0.1/moviesdata.user").\
            load()

readUsers.printSchema()

readUsers.createOrReplaceTempView("users")

sqlDF = spark.sql("""
SELECT occupation,count(user_id) cnt_usr
FROM users 
GROUP BY occupation
ORDER BY cnt_usr DESC
""")

sqlDF.show()