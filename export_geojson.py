import sys
import geojson
from parse import data_scrub
from prototype import (
    TEST_FILES,
    SLOW_SIGNALS,
    SIGNALS,
)

_, gps_points, _ = data_scrub(TEST_FILES[0], SLOW_SIGNALS, SIGNALS, slow_freq=0.2, fast_freq=0.01)

track = geojson.LineString(gps_points)

features = list()
features.append(geojson.Feature(geometry=track, properties={"Test Name": "Sample Track"}))

feature_collection = geojson.FeatureCollection(features)

with open('test.geojson', mode='w') as output:
    geojson.dump(feature_collection, output)
