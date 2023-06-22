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
        .filter(sflux.ROW('_measurement') == 'your_measurement')\
        .pivot()\
        .filter(sflux.ROW('your_column') != 'something')\
        .to_dict()
```

### 2. Writing data
The writing of time series into the database is acheived by building measurement objects for each measurement to be
 inserted:

```python
import time
import sflux

measurements = [
    sflux.Measurement('measurement_name', field1='value1', field2='value2')
        .tags(tag1='tag1', tag2='tag2')
        .time(time.time())
]
with sflux.Client(url='your_host', token='your_token', org='your_org') as client:
    client.write('your_bucket', measurements)

```

### 3. Usage considerations
This library is still under development and should be used with care. Please report any bugs as
issues and they will be addressed promptly.

For now Sflux does not implement string checks to avoid Injections. Please always verify any user
input before passing it to Sflux. String checking will be implemented in future releases.