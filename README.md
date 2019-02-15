# RainyRide
## Rainy Ride: Finding passenger hotspot in rainy day
## Business Value
It's hard to get a ride at rainy day. It's not only because the bad traffic, but also there are some potential passengers only take ride in rainy day. Where are they? This project want to predict the hotspot of passengers in rainy day, also provide an insight of potential profit and distance for drivers, based on historical taxi transaction data and precipitation data.
<br>
## User Interface
![alt text](https://github.com/qinyuyue/RainyRide/blob/master/main_page.png)
<br>
When user click the point, it will the locations, predicted price and predicted distance of that point.
## Data
<pre>
| ------------------------------------------------------- | ----- | ---------------------------------------------------- |<br>
|                           Datasets                      |  Size |                      File Format                     |<br>
| ------------------------------------------------------- | ----- | ---------------------------------------------------- |<br>
| New York city yellow taxi transaction records, 2009-2013| ~70GB | csv, monthly records per file, 10M records per month |<br>
| ------------------------------------------------------- | ----- | ---------------------------------------------------- |<br>
| New York city precipitation records, 2009-2013          | ~2MB  | csv, total records in one file                       |<br>
| ------------------------------------------------------- | ----- | ---------------------------------------------------- |<br>
</pre>
New York yellow taxi transaction data: https://registry.opendata.aws/nyc-tlc-trip-records-pds/<br>
Precipitation data: https://www.ncdc.noaa.gov/cdo-web/
<br>
## Tech Stack
![alt text](https://github.com/qinyuyue/RainyRide/blob/master/pipeline.png)
<br>
Basically, New York yellow taxi transaction dataset is an open source on aws. The precipitation data was uploaded to aws. Then Spark was choosed for batch processing. Why Spark? An iterative algorithm (DBSCAN) is the most time-consuming part in this computing process. The running time is much shorter in Spark than Hadoop. After the statistical result got, it was stored at MySQL, which is easy to handle relational table. Later, the result was display on web page, which built by Flask and Leaflet.    
<br>
## Cluster Structure
One cluster with 6 instance.
Flask node.
