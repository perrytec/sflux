import json
import logging
import datetime

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.flux_table import TableList

from sflux.utils import parse_to_string
from sflux.measurement import Measurement

try:
    import pandas as pd
except ImportError:
    pd = None

logger = logging.getLogger('sflux')


class Client(InfluxDBClient):
    def __init__(self, url: str, token: str, org: str, **kwargs):
        super().__init__(url=url, token=token, org=org, **kwargs)
        self.url = url
        self.token = token
        self.org = org

    def query(self, bucket: str):
        """
        Equivalent to the FROM statement in FluxQL
        :param bucket:      Name of the bucket to be queried
        """
        return _Query.new(self, bucket)

    def insert(self, bucket: str, measurements: (list, tuple), write_mode: str = 'SYNCHRONOUS'):
        """
        Inserts a list of measurement objects to influx
        :param bucket:       Bucket where the measurements will be inserted
        :param measurements: List of MEASUREMENT objects
        :param write_mode:   Can either be SYNCHRONOUS or ASYNCHRONOUS as defined in influxdb_client
        """
        if isinstance(measurements, Measurement):
            measurements = [measurements]
        if write_mode == 'SYNCHRONOUS':
            options = {'write_options': SYNCHRONOUS}
        else:
            options = {'success_callback': self.on_write_success,
                       'error_callback': self.on_write_error,
                       'retry_callback': self.on_write_retry}

        with self.write_api(**options) as write_api:
            write_api.write(bucket=bucket, org=self.org, record='\n'.join(str(elem) for elem in measurements))

    def on_write_success(self, conf: (str, str, str), data: str):
        pass

    def on_write_error(self, conf: (str, str, str), data: str, exception: Exception):
        pass

    def on_write_retry(self, conf: (str, str, str), data: str, exception: Exception):
        pass

    def check_health(self):
        return self.ping()


def add_to_query(func):
    """
    Decorator to return a new _Query object out of the original query
    """
    def inner(self, *args, **kwargs) -> "_Query":
        query_addition = func(self, *args, **kwargs)
        if isinstance(query_addition, str):
            query_addition = [query_addition]
        elif isinstance(query_addition, (tuple, list)):
            pass
        else:
            raise ValueError(f'Invalid query_addition type: {type(query_addition)}')
        return _Query(client=self._client, components=self._components + query_addition)
    return inner


class _Query:
    def __init__(self, client: InfluxDBClient, components: list):
        """
        Represents an influx query that is being prepared and not yet sent
        """
        self._components = components
        self._client = client

    @classmethod
    def new(cls, client: InfluxDBClient, bucket: str) -> "_Query":
        comps = [f'from(bucket: "{bucket}")']
        return cls(components=comps, client=client)

    #######################################
    # Query methods
    #######################################
    @add_to_query
    def range(self, start: (int, str, datetime.datetime), stop: (int, str, datetime.datetime) = None) -> str:
        """
        Adds a specified timerange to the current query. This range can be either relative or absolute, which will
        depend on the start argument datatype.

        If start is a string (for example: '-15d') the type will be considered relative.
        If start is an int or datetime (for example: 1625659548) it will be considered a absolute point in time.
        If 'stop' key is not present or its value is None, now() will be considered as default. If 'start' key is not
        present method will raise an error.
        """
        v_start, v_stop = self._validate_range(start, stop)
        return f'|> range(start: {v_start}, stop: {v_stop})'

    @add_to_query
    def filter(self, condition: str) -> str:
        """
        Implements the filter function from FluxQL

        :param condition: String with the condition to filter by. To avoid having to write the strings one should use
                          the helper ROW class
        """
        return f'|> filter(fn: (r) => {condition})'

    @add_to_query
    def pivot(self,
              rows: (str, list, tuple) = '_time',
              columns: (str, list, tuple) = '_field',
              value: str = '_value') -> str:
        """
        Implements the PIVOT function from FluxQL
        :param rows: Rows to pivot by
        :param columns: Columns to pivot by
        :param value: Value to pivot by
        """
        if isinstance(rows, str):
            rows = [rows]
        if isinstance(columns, str):
            columns = [columns]
        return f'|> pivot(rowKey: {parse_to_string(rows)}, columnKey: {parse_to_string(columns)}, valueColumn: "{value}")'

    @add_to_query
    def group(self, columns: (str, list, tuple) = None, mode: str = None) -> str:
        """
        Implements the GROUP function from FluxQL
        """
        if columns is not None:
            if isinstance(columns, str):
                columns = [columns]
            column_component = f'columns: {parse_to_string(columns)}'
        else:
            column_component = ''

        if mode is not None:
            mode_component = f', mode: "{mode}"'
        else:
            mode_component = ''

        return f'|> group({column_component}{mode_component})'

    @add_to_query
    def sort(self, columns: (str, list, tuple), desc: bool = False) -> str:
        """
        Implements the SORT function from FluxQl
        """
        if isinstance(columns, str):
            columns = [columns]
        return f'|> sort(columns: {parse_to_string(columns)}, desc: {parse_to_string(desc)})'

    @add_to_query
    def limit(self, n: int = 10, offset: int = 0) -> str:
        """
        Implements the LIMIT function from FluxQL
        """
        return f'|> limit(n: {n}, offset: {offset})'

    @add_to_query
    def last(self) -> str:
        """
        Implements the LAST function from FluxQL
        """
        return '|> last()'

    @add_to_query
    def drop(self, columns: list) -> str:
        """
        Implements the DROP function from FluxQL
        """
        return f'|> drop(columns: {parse_to_string(columns)})'

    @add_to_query
    def mean(self, column: str = '_value') -> str:
        """
        Implements the MEAN function from FluxQL
        """
        return f'|> mean(column: "{column}")'

    @add_to_query
    def std(self, column: str = '_value', mode: str = 'sample') -> str:
        """
        Implements the MEAN function from FluxQL
        """
        return f'|> stddev(column: "{column}", mode: "{mode}")'

    @add_to_query
    def count(self, column: str = '_value') -> str:
        """
        Implements the MEAN function from FluxQL
        """
        return f'|> count(column: "{column}")'

    @add_to_query
    def map(self, operations: dict, keep_original: bool = True) -> str:
        """
        Implements the MAP function from FluxQL
        :param operations: Dictionary of operations. Each key is the name of the new column and the item is a operation
                           of ROW objects. Example: {column: ROW('first') + ROW('second')}
        :param keep_original: True if the original columns need to be kept, False otherwise
        """
        starter = ' r with ' if keep_original else ''
        components = ', '.join([f'{key}: {operations[key]}' for key in operations])
        return '|> map(fn: (r) => ({ %s %s }))' % (starter, components)

    #######################################
    # Execution methods
    #######################################
    def all(self) -> TableList:
        """
        Returns the results of the query as a list of influx tables. Each table has records that contain the values.

        Example:
            for table in result:
                for record in table.record:
                    print(record.values)
        """
        return self._client.query_api().query('\n'.join(self._components))

    def to_dataframe(self) -> "pd.DataFrame":
        """
        Returns the results of the query in a list of pandas dataframes
        """
        return self._client.query_api().query_data_frame('\n'.join(self._components))

    def to_dict(self) -> list:
        """
        Returns the results as a pandas friendly list of dictionaries
        """
        results = self.all()
        output = []
        for table in results:
            for record in table.records:
                output.append(record.values)
        return output

    #######################################
    # Helper methods
    #######################################
    def _validate_range(self,  start: (int, float, str, datetime.datetime), stop: (int, float, str, datetime.datetime)):
        if stop is None:
            stop = 'now()'
        elif isinstance(stop, float):
            stop = int(stop)
        elif isinstance(stop, int) or isinstance(stop, str):
            pass
        elif isinstance(stop, datetime.datetime):
            stop = self._dt_to_rfc3339(stop)
        else:
            raise ValueError(f"_type {type(stop)} not recognized. ")
        if start is None:
            raise ValueError(f"Invalid start value. ")
        elif isinstance(start, float):
            start = int(start)
        elif isinstance(start, int) or isinstance(start, str):
            pass
        elif isinstance(start, datetime.datetime):
            start = self._dt_to_rfc3339(start)
        else:
            raise ValueError(f"_type {type(start)} not recognized. ")
        return start, stop

    def _dt_to_rfc3339(self, datetime_obj: datetime.datetime = datetime.datetime.now(), _format: str = 'long'):
        """
        Transform datetime object into string RFC3339 format (either in date, short or long format). Ignores
         timezone aware datetime objects.
         """
        if datetime_obj is not None:
            base = datetime_obj.isoformat()
            res = self._get_resolution(base)
            base = base.split('+')[0] if '+' in base else base
            if _format == 'date':
                return base.split('T')[0]
            elif _format == 'short':
                return base[:-res-1] + 'Z' if res == 6 else base + 'Z'
            elif _format == 'long':
                return base[:-res//2] + 'Z' if res == 6 else base + '.000Z'
            else:
                raise ValueError("Enter a format from 1 to 3")

    @staticmethod
    def _get_resolution(isoformat):
        if len(isoformat.split('.')) == 1:
            return -1
        else:
            return len(isoformat.split('.')[1])
