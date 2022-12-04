# SFLUX

Simple Influx DB wrapper for python.
Builds on top of influxdb-client to make the use of Influxdb as simple as possible

## Basic Usage
### 1. Querying data
Sflux transforms the FluxQL syntaxis into intuitive python commands that try 
to follow FluxQL as close as possible. A simple query looks like this:

```python
import sflux

with sflux.Client(url='your_host', token='your_token', org='your_org') as client:
    result = client.query(bucket='your_bucket')\
        .range(start='-1d')\
        .filter(sflux.ROW('measurement') == 'your_measurement')\
        .pivot()\
        .filter(sflux.ROW('your_column') != 'something')\
        .to_dict()
```

### 2. Writing data
The write methods are still being tested and will be fully supported soon.