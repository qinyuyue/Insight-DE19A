from flask import Flask, render_template
from flask_mysqldb import MySQL
from geojson import Feature, FeatureCollection, Point
import pandas as pd, json
import yaml
# configure mysql connection
db = yaml.load(open('db.yml'))
app = Flask(__name__)
app.config['MYSQL_USER'] = db['user']
app.config['MYSQL_PASSWORD'] = db['passwd']
app.config['MYSQL_DB'] = db['rainyride']
app.config['MYSQL_HOST'] = db['host']
mysql = MySQL(app)

@app.route('/')
# input data from MySQL database
def load():
    cur = mysql.connection.cursor()
    # monthly average rain days for New York is 10, filter out the counts greater than 30 in rain day
    cur.execute('SELECT * FROM op_stats WHERE counts/10 > 30')
    df = cur.fetchall()
    return df
# convert data to GeoJson formart
    features = []
    geojson = {'type':'FeatureCollection', 'features':[]}
    properties = ["point", "distance"]
    for x in df:
        feature = {'type':'Feature',
                       'properties':{},
                       'geometry':{'type':'Point',
                                   'coordinates':[]}}
        feature['geometry']['coordinates'] = [x[6],x[5]]
        feature['properties']['price'] = "Price: $" + str(x[2])
        feature['properties']['distance'] = "Distance: $" + str(x[4])
        geojson['features'].append(feature)

    return render_template("hotspot.html", title="RainyRide: Passenger Hotspot in Rainy Day", points=geojson)

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000)
