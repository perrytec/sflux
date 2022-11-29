class Measurement:
    def __init__(self, name: str, fields: dict, *, tags: dict = None, _time: int = None):
        """
        Represents a basic influx measurement
        :param name:   Name of the measurement
        :param fields: Dictionary with fields of the measurement
        :param tags:   Dictionary with tags of the measurement
        :param _time:  Time of the measurement. Can be in s, ms, us or ns. If passing a float the nanosecond precision
                       will be lost due to floating point accuracy limitations.
        """
        self.name = name
        self.tags = tags
        self.fields = fields

        if not isinstance(_time, (type(None), float, int)):
            raise ValueError(f'Invalid time type: {type(_time)}')
        self._time = _time

    def __repr__(self):
        if self._time is not None and self.tags is not None:
            return f'{self.name},{self._parse_tags()} {self._parse_fields()} {self._parse_time()}'
        elif self._time is not None:
            return f'{self.name} {self._parse_fields()} {self._parse_time()}'
        elif self.tags is not None:
            return f'{self.name},{self._parse_tags()} {self._parse_fields()}'
        else:
            return f'{self.name} {self._parse_fields()}'

    def _parse_tags(self):
        return ",".join([f"{key}={self.tags[key]}" for key in self.tags])

    def _parse_fields(self):
        return ",".join([f"{key}={self.fields[key]}" for key in self.fields])

    def _parse_time(self):
        """Parses time to nanoseconds"""
        if self._time > 1e18:
            # Is in nanos
            multiplier = 1
        elif self._time > 1e15:
            # Is in micros
            multiplier = 1e3
        elif self._time > 1e12:
            # Is in milis
            multiplier = 1e6
        elif self._time > 1e9:
            # Is ins seconds
            multiplier = 1e9
        else:
            raise ValueError(f'Time needs to be at least in seconds but received {self._time}')
        return str(round(self._time * multiplier))
