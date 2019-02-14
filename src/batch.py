from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
import datetime
from sklearn.cluster import DBSCAN
import numpy as np
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as func

def load_df(file, columns):
    """load csv file as dataframe
    Read the csv file with it's headers, automatically generate schema by
    databricks package.

    Args:
        file: string. Input filename with its path
        column: list. the column name want to be select
    """
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file)
##
    sel_df = df.select(columns)
    return sel_df

def time2hour(df):
    """round taxi data tiem to nearest hour"""
    df_time = (round(unix_timestamp(sel_df.columns[0]) / 3600) * 3600).cast("timestamp")
    cleaned_df = df.withColumn("Trip_Pickup_DateTime", df_time)
    return cleaned_df


def clean_weather(df):
    df['DATE'] = df['DATE'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:] + ':00')
    df['HPCP'] = df['HPCP'].mask(df['HPCP'] > 0, 1)
    df1.to_csv('./cleaned_rain.csv', index=False)

# def load2sparkdf(file):
#     df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file)
#     return df

def to_time(time):
    """ convert time fomart in rainy records """
    x = str(x)
    timestamp = 'DATE'
    if len(x) == 14:
        x = x[:4] + '-' + x[4:6] + '-' + x[6:] + ':00'
        new_time= datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
    return new_time
spark.udf.register("to_timeWithPython", to_time, TimestampType())

def join2(df1, df2, flag=1):
    """ Join two dataset and clean up
    Join two dataset by time and filled out NA rows.
    Filter the result with rain flag, which is HPCP == 1.
    Clean up the geo-location out of New York city.
    Filter out the Trip_Distance and Total_Amt < 0.
    """

    joined = df1.join(df2, df1.Trip_Pickup_DateTime == sel_df2.DATE, how='inner')
    joined.dropna()
    rain = joined.filter(joined.HPCP == 1)
    at_fil = rain.filter((rain.Start_Lat < 40.81) &  (rain.Start_Lat > 40.45))
    log_fil = lat_fil.filter((lat_fil.Start_Lon > -74.3) & (lat_fil.Start_Lon < -73.7))
    filtered = log_fil.filter(log_fil.Trip_Distance > 0)
    filtered = filtered.filter(filter.Total_Amt > 0)

    return filtered

def roundto3(df, col_org, col_new):
    rounded = filtered.withColumn("r_lat", func.round(df1["Start_Lat"], 3))
    return rounded

def cluster_loc(df,min_sample=1):
    # coords = df.as_matrix(columns=['Start_Lat', 'Start_Lon'])
    ## above not work in spark
    coords = np.array(df1.select("Start_Lat","Start_Lon").collect())
    kms_per_radian = 6371.0088
    epsilon = 1.5 / kms_per_radian
    dbscan = DBSCAN(eps=epsilon, min_samples=100, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
    cluster_labels = db.labels_
    df_out = pd.DataFrame(clustering.labels_, columns = ['clusters'])
    return df_out


def main():
    taxi_file = input_argument
##
    rain_file = "s3a://ny-taxi/cleaned_rain.csv"
    taxi_columns = ["pickup_datetime", "pickup_latitude", "pickup_longitude", "total_amount"]
    rain_columns = ["DATE", "HPCP"]

    df1 = load_df(taxi, taxi_columns)
    df1 = time2hour(df1)

    df2 = load_df(rain_file, rain_column)

    joined = join2(df1,df2)

    rounded = joined.withColumn("r_lat", func.round(df1["Start_Lat"], 3))
    rounded = rounded.withColumn("r_lon", func.round(df1["Start_Lon"], 3))
    clusterd = cluster_loc(rounded)

    output =  clusterd.groupby('r_lat', 'r_lon').agg(func.mean('Total_Amt'),func.count('Total_Amt'))
    indexs = output.withColumn("id", monotonically_increasing_id())
    indexs.write.format("jdbc").option('url', 'jdbc:mysql://localhost/rainyride').option('driver','com.mysql.cj.jdbc.Driver').option("dbtable", "op_stats").option("user", "root").option("password", "rainyride").mode('overwrite').save()


if __name__ == '__main__':
  main()
