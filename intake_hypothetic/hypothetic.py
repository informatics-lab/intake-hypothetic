from datetime import datetime, timedelta
import importlib
import itertools
import warnings
import urllib

import iris
import iris_hypothetic
import pandas as pd
import numpy as np
import tempfile
import boto3
from botocore.handlers import disable_signing

from intake.source.base import DataSource, Schema

from . import __version__


SECONDS_IN_HOUR = 60 * 60
SECONDS_IN_DAY = 60 * 60 * 24


def _import_from(module, name):
    module = __import__(module, fromlist=[name])
    return getattr(module, name)


def _product_dict(**kwargs):
    keys = kwargs.keys()
    vals = kwargs.values()
    for instance in itertools.product(*vals):
        yield dict(zip(keys, instance))


class HypotheticSource(DataSource):
    """Intake hypothetic"""
    version = __version__
    container = 'iris'
    name = 'hypothetic'
    partition_access = True

    def __init__(self, key_generator=None, forecast_reference_time=None, iris_kwargs=None, metadata=None, storage_options=None,
                 **kwargs):
        self.key_generator = key_generator
        self.forecast_reference_time = forecast_reference_time
        self._kwargs = iris_kwargs or kwargs
        self.metadata = metadata
        self.metadata_df = None
        self._template_cube_file = None
        self.storage_options = storage_options
        self._ds = None
        super(HypotheticSource, self).__init__(metadata=metadata)

    def _open_dataset(self):
        self.metadata_df = self.generate_metadata()
        self._template_cube_file, _ = self.find_template_cube(None)
        uris = self.metadata_df.uri
        replacement_coords = self.extract_unique_metadata(['uri'])
        hypotheticube = iris_hypothetic.load_hypotheticube(self._template_cube_file.name, self.metadata['name'], replacement_coords, uris, storage_options=self.storage_options)
        self._ds = hypotheticube

    def _open_as_local(self, path):
        if path.startswith('s3://'):
            bucket, key = path[len('s3://'):].split('/', 1)
            s3 = boto3.resource('s3')

            if self.storage_options and self.storage_options.get('anon', False):
                s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

            try:
                object_body = s3.Bucket(bucket).Object(key).get()['Body'].read()
            except s3.meta.client.exceptions.NoSuchKey:
                raise IOError(f'No such file {path}')

            file = tempfile.NamedTemporaryFile()
            file.write(object_body)
            file.seek(0)

            return file

        if path.startswith('http://') or path.startswith('https://'):
            object_body = urllib.request.urlopen(path).read()
            file = tempfile.NamedTemporaryFile()
            file.write(object_body)
            file.seek(0)

            return file

        return open(path, 'rb')

    def _get_schema(self):
        """Make schema object, which embeds iris object and some details"""
        if self._ds is None:
            self._open_dataset()

            metadata = {}
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata)
        return self._schema

    def read(self):
        """Return iris object (which will have chunks)"""
        return self.read_chunked()

    def read_chunked(self):
        """Return iris object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def read_partition(self, i):
        """Fetch one chunk of data at tuple index i
        """

        import numpy as np
        self._load_metadata()
        if not isinstance(i, (tuple, list)):
            raise TypeError('For iris sources, must specify partition as '
                            'tuple')
        if isinstance(i, list):
            i = tuple(i)
        if isinstance(self._ds, iris.cube.CubeList):
            arr = self._ds[i[0]].lazy_data()
            i = i[1:]
        else:
            arr = self._ds.lazy_data()
        if isinstance(arr, np.ndarray):
            return arr
        # dask array
        return arr[i].compute()

    def to_dask(self):
        """Return iris object where variables are dask arrays"""
        return self.read_chunked()

    def close(self):
        """Delete open file from memory"""
        self._ds = None
        self._schema = None

    @staticmethod
    def generate_frts(frt_description):
        now = datetime.now()
        interval = frt_description['forecast_reference_time_interval']
        model_start_time = frt_description['model_start_time']
        retention = frt_description['retention']

        # Number of runs per day
        runs_per_day = int(SECONDS_IN_DAY / interval)

        # Hours of the day which the model runs (e.g midnight, 3am, 6am, etc)
        run_hours = [model_start_time + (interval * i) / SECONDS_IN_HOUR for i in range(0, runs_per_day)]

        # Get the last run relative to now
        last_run_hour = max([x for x in run_hours if x <= now.hour])

        # Create a datetime object for the last run
        final_run = now.replace(minute=0, second=0, microsecond=0, hour=int(last_run_hour))

        # Create a generator of all the runs going back as far as the retention
        runs_generator = ((final_run - timedelta(seconds=i*interval)).strftime("%Y-%m-%dT%H:%M:%SZ") for i in range(0, int(retention/interval)))

        return runs_generator

    def generate_metadata(self):
        generator_module_name = ".".join(self.key_generator.split('.')[:-1])
        generator_function_name = self.key_generator.split('.')[-1]
        generator_function = _import_from(generator_module_name, generator_function_name)

        self.metadata['forecast_reference_time'] = list(self.generate_frts(self.forecast_reference_time))

        iter_metadata = {key: value for key, value in self.metadata.items() if isinstance(value, list)}
        scalar_metadata = {key: value for key, value in self.metadata.items() if not isinstance(value, list)}

        df = pd.DataFrame.from_dict([{**product_dict, **scalar_metadata} for product_dict in _product_dict(**iter_metadata)])

        df['uri'] = df.apply(lambda row: generator_function({k: str(int(v)) if isinstance(v, np.int64) else str(v) for k, v in row.to_dict().items()}), axis=1)
        return df

    def find_template_cube(self, var_name):

        for index, row in self.metadata_df.iterrows():
            test_metadata = row.to_dict()
            path = test_metadata['uri']

            try:
                file = self._open_as_local(path)
                cube = iris.load_cube(file.name, var_name)
            except (IOError, OSError):
                continue
            else:
                return file, cube

        raise ValueError("Failed to find template cube")

    def extract_unique_metadata(self, drop):
        replacement_coords = self.metadata_df
        replacement_coords = self.metadata_df.drop(drop, axis=1)
        nunique = replacement_coords.apply(pd.Series.nunique)
        cols_to_drop = nunique[nunique == 1].index
        return replacement_coords.drop(cols_to_drop, axis=1)
