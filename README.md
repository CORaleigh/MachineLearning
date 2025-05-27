## Moved Sardeenz old readme.md to the garbageDetector directory as it was meant for that

This repo contains some proof of concept efforts around garbage detection, using a raspberry pi for detection, TensorFlow, and Unsupervised machine learning. It also contains the production script timedelta_directioncount.py (and _new).


timedelta_directioncount.py

Consumes the Kafka topic for the vehicle turning movement count data stream. Sums the detections over 15 minute time periods and then pushes that to another Kafka Topic for Esri GeoEvent to consume for ArcGIS Online dashboards.


timedelta_directioncount_new.py

Similar to timedelta_directioncount.py, but for a more complicated input data stream that also contains pedestrian crosswalk counts, duration, bikes, and near misses. Aggregates that data and pushes to a new topic for GeoEvent.


/garbageDetector

An early proof of concept garbage on sidewalk detector.


/raspberrypi

Just some notes on running TensorFlow on a raspberrypi.


/VehicleCountingTensorFlow

An old copy of this repo: https://github.com/ahmetozlu/vehicle_counting_tensorflow


/Unsupervised

Contains a python notebook intended to use some open source libraries like sci-kit learn and sanborn to automatically review data and provide inference that may not have been thought of by a person.