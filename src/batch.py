from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import datetime
from sklearn.cluster import DBSCAN
import numpy as np

sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId","AKIAIPYAFA37TXCTFGDA")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

# file = "s3a://ny-taxi/yellow_tripdata_2017-01.csv"
file = "s3a://nyc-tlc/trip\ data/yellow_tripdata_2013-08.csv"
data = spark.sql("SELECT * FROM csv.`file`").cache()

# df = sqlContext.read.format("csv").option("header", "true").load("s3a://nyc-tlc/trip\ data/yellow_tripdata_2013-08.csv")
df1 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("s3a://nyc-tlc/trip\ data/yellow_tripdata_2009-01.csv")
#2013
sel_df1 = df1.select("pickup_datetime", "pickup_latitude", "pickup_longitude", "total_amount")
# sel_df1 = df1.select("Trip_Pickup_DateTime","Start_Lat","Start_Lon","Total_Amt","Trip_Distance")
df2 = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("s3a://ny-taxi/cleaned_rain.csv")
sel_df2 = df2.select("DATE", "HPCP")
df2_test = sel_df2.limit(10)
# 2009
sel_df3 =  df3.select("Trip_Pickup_DateTime", "Start_Lat", "Start_Lon", "Total_Amt","Trip_Distance")


# round time to nearest hour
df_time = (round(unix_timestamp(sel_df1.columns[0]) / 3600) * 3600).cast("timestamp")
cleaned_df1 = sel_df1.withColumn("Trip_Pickup_DateTime", df_time)
## HOW TO CHANGE VALUE DIRECTLY RATHER THAN STORE THEM IN A NEW VALUE

def clean_weather(df):
    df['DATE'] = df['DATE'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:] + ':00')
    df['HPCP'] = df['HPCP'].mask(df['HPCP'] > 0, 1)
    df1.to_csv('./cleaned_rain.csv', index=False)

def load2sparkdf(file)
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file)
    return df

def to_time(x):
    x = str(x)
    timestamp = 'DATE'
    if len(x) == 14:
        x = x[:4] + '-' + x[4:6] + '-' + x[6:] + ':00'
        timestamp = datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    return timestamp
spark.udf.register("to_timeWithPython", to_time, TimestampType())
# spark.udf.register("squareWithPython", square, FloatType())

joined = cleaned_df1.join(sel_df2, cleaned_df1.Trip_Pickup_DateTime == sel_df2.DATE, how='inner')

# df_time = (round(unix_timestamp(sel_df1.columns[0]) / 3600) * 3600).cast("timestamp")
# cleaned_df1 = sel_df1.withColumn("Trip_Pickup_DateTime", df_time)

joined.dropna()
joined.filter(joined.HPCP == 1).count()
# 1067493
rain = joined.filter(joined.HPCP == 1)
#pos_sort = rain.orderBy(["Start_Lat", "Start_Lon"], ascending = [1,1])
# sort fast, but pos_sort() slow
lat_fil = rain.filter((rain.Start_Lat < 40.81) &  (rain.Start_Lat > 40.45))
log_fil = lat_fil.filter((lat_fil.Start_Lon > -74.3) & (lat_fil.Start_Lon < -73.7))
filtered = log_fil.filter(log_fil.Trip_Distance > 0)
filtered = filtered.filter(filter.Total_Amt > 0)

output =  filtered.groupby('r_lat', 'r_lon').agg(func.mean('Total_Amt'),func.count('Total_Amt'))
# ouput as csv
log_fil.write.format("com.databricks.spark.csv").option("header", "true").save("0901loc.csv")
# def cluster_loc(df,min_sample=100):
#     # coords = df.as_matrix(columns=['Start_Lat', 'Start_Lon'])
#     ## above not work in spark
#     coords = np.array(df1.select("Start_Lat","Start_Lon").collect())
#     kms_per_radian = 6371.0088
#     epsilon = 1.5 / kms_per_radian
#     dbscan = DBSCAN(eps=epsilon, min_samples=100, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
#     cluster_labels = db.labels_
#     df_out = pd.DataFrame(clustering.labels_, columns = ['clusters'])
## output to mysql
df2.write.format("jdbc").option('url', 'jdbc:mysql://localhost/mydb').option('driver','com.mysql.cj.jdbc.Driver').option("dbtable", "op_stats").option("user", "root").option("password", "raintaxi").mode('overwrite').save()

MySQL  
SELECT *
    -> FROM op_stats
    -> ORDER BY counts DESC
    -> LIMIT 10;
