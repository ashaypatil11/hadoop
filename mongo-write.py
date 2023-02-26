from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[5])

spark = SparkSession.\
    builder.appName("mongo-spark").\
        getOrCreate()

lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/mongodb/movies.user")

users = lines.map(parseInput)

userDataset = spark.createDataFrame(users)

userDataset.write.\
    format("com.mongodb.spark.sql.DefaultSource").\
        option("uri", "mongodb://127.0.0.1/moviesdata.user").\
            mode("overwrite").\
                save()
