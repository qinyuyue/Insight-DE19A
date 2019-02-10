from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

spark = SparkSession.builder.appName('ReadData').getOrCreate()

df = sqlContext.read.format("jdbc").options(
    url='jdbc:mysql://ec2-34-217-125-148.us-west-2.compute.amazonaws.com:3306/rainyride',
    driver = 'com.mysql.cj.jdbc.Driver',
    dbtable = 'op_stats',
    user = 'root',
    password = 'rainyride').load()
df.show()

df = sqlContext.read.format("jdbc").option('url', 'jdbc:mysql://ec2-34-217-125-148.us-west-2.compute.amazonaws.com:3306/rainyride').option('driver','com.mysql.cj.jdbc.Driver').option("dbtable", "op_stats").option("user", "root").option("password", "rainyride").load()
df = sqlContext.read.format("jdbc").option('url', 'jdbc:mysql://localhost/mydb').option('driver','com.mysql.cj.jdbc.Driver').option("dbtable", "op_stats").option("user", "root").option("password", "raintaxi").load()


CREATE TABLE op_stats
    (location_id INT NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    avg_price FLOAT NOT NULL,
    avg_distance FLOAT NOT NULL,
    counts INT NOT NULL,
    PRIMARY KEY (location_id));
