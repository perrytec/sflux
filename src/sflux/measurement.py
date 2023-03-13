import math


class Measurement:
    def __init__(self, name: str, **fields):
        """
        Represents a basic influx measurement
        :param name:   Name of the measurement
        :param fields: Fields of the measurement
        :param _time:  Time of the measurement. Can be in s, ms, us or ns. If passing a float the nanosecond precision
                       will be lost due to floating point accuracy limitations.
        """
        self.name = name
        self.fields = fields
        self._tags = None
        self._time = None

    def tags(self, **tags):
        self._tags = tags
        return self

    def time(self, _time):
        if not isinstance(_time, (type(None), float, int)):
            raise ValueError(f'Invalid time type: {type(_time)}')
        self._time = _time
        return self

    def __repr__(self):
        if self._time is not None and self.tags is not None:
            return f'{self.name},{self._parse_tags()} {self._parse_fields()} {self._parse_time()}'
        elif self._time is not None:
            return f'{self.name} {self._parse_fields()} {self._parse_time()}'
        elif self._tags is not None:
            return f'{self.name},{self._parse_tags()} {self._parse_fields()}'
        else:
            return f'{self.name} {self._parse_fields()}'

    def _parse_tags(self):
        return ",".join([f"{key}={self._tags[key]}" for key in self._tags])

    def _parse_fields(self):
        return ",".join([f"{key}={self._parse_field_value(self.fields[key])}"
                         for key in self.fields if self._field_is_valid(self.fields[key])])

    @staticmethod
    def _field_is_valid(value):
        """
        Checks if the value of the given field is valid. None, Inf and NaN are not valid in influx.
        """
        if value is None:
            return False

        if isinstance(value, float):
            if math.isinf(value):
                return False

            if math.isnan(value):
                return False

        return True

    @staticmethod
    def _parse_field_value(value):
        """Parses the value of each independent field. Puts quotes around it if it is a string"""
        if isinstance(value, str):
            return f'"{value}"'
        return value

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
