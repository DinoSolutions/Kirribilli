import geojson
from parse import data_scrub
from prototype import (
    TEST_FILES,
    SLOW_SIGNALS,
    SIGNALS,
)

gps_time, gps_points, gps_boundaries = data_scrub(TEST_FILES[0], SLOW_SIGNALS, SIGNALS, slow_freq=1, fast_freq=0.01)

track = geojson.LineString(gps_points)

features = list()
features.append(geojson.Features(geometry=LineString, properties={"Test Name": "Huron River Dr"}))

feature_collection = geojson.FeatureCollection(features)

with open('test.geojson', 'w') as output:
    geojson.dump(feature_collection, output)

