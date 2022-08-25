# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import abc
import argparse
import contextlib
import dataclasses
import logging
import shutil
import tempfile
import typing as t
import os
import io
import apache_beam.io.gcp.gcsio
import xarray

from pyproj import Transformer
import numpy as np
import datetime
from google.api_core.exceptions import NotFound
import apache_beam as beam
import rasterio
import xarray as xr
import cfgrib
from apache_beam.io.filesystems import FileSystems
from google.cloud import storage
from subprocess import Popen, PIPE, STDOUT
from apache_beam.io.gcp.gcsio import DEFAULT_READ_BUFFER_SIZE
from apache_beam.io.gcp.gcsio import GcsIO
from urllib.parse import urlparse
import pathlib
from apache_beam.io.filesystem import CompressionTypes
from cfgrib.dataset import DatasetBuildError
from hashlib import sha256
from codetiming import Timer
import time
import fsspec
import subprocess
import threading

from .util import (
    DEFAULT_COORD_KEYS,
    _only_target_vars,
    convert_size
)
import socket
import tenacity
import google.auth
from zarr.errors import GroupNotFoundError
CREDS, PROJECT = google.auth.default()
CUSTOM_READ_BUFFER_SIZE = 1 * (1 << 20)
HOSTNAME = socket.gethostname()
TIF_TRANSFORM_CRS_TO = "EPSG:4326"
# A constant for all the things in the coords key set that aren't the level name.

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclasses.dataclass
class ToDataSink(abc.ABC, beam.PTransform):
    dry_run: bool

    @classmethod
    def from_kwargs(cls, **kwargs):
        fields = [f.name for f in dataclasses.fields(cls)]
        return cls(**{k: v for k, v, in kwargs.items() if k in fields})

    @classmethod
    @abc.abstractmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser) -> None:
        pass

    @classmethod
    @abc.abstractmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_options: t.List[str]) -> None:
        pass


def _make_grib_dataset_inmem(grib_ds: xr.Dataset) -> xr.Dataset:
    """Copies all the vars in-memory to reduce disk seeks every time a single row is processed.

    This also removes the need to keep the backing temp source file around.
    """
    data_ds = grib_ds.copy(deep=True)
    for v in grib_ds.variables:
        if v not in data_ds.coords:
            data_ds[v].variable.values = grib_ds[v].variable.values
    return data_ds


def _preprocess_tif(ds: xr.Dataset, filename: t.Union[str, None], tif_metadata_for_datetime: str, rasterio_data_array: rasterio.DatasetReader = None) -> xr.Dataset:
    """Transforms (y, x) coordinates into (lat, long) and adds bands data in data variables.

    This also retrieves datetime from tif's metadata and stores it into dataset.
    """

    def _get_band_data(i):
        band = ds.band_data[i]
        band.name = ds.band_data.attrs['long_name'][i]
        return band

    y, x = np.meshgrid(ds['y'], ds['x'])
    transformer = Transformer.from_crs(ds.spatial_ref.crs_wkt, TIF_TRANSFORM_CRS_TO, always_xy=True)
    lon, lat = transformer.transform(x, y)

    ds['y'] = lat[0, :]
    ds['x'] = lon[:, 0]
    ds = ds.rename({'y': 'latitude', 'x': 'longitude'})

    band_length = len(ds.band)
    ds = ds.squeeze().drop_vars('band').drop_vars('spatial_ref')

    band_data_list = [_get_band_data(i) for i in range(band_length)]

    ds_is_normalized_attr = ds.attrs['is_normalized']
    ds = xr.merge(band_data_list)
    ds.attrs['is_normalized'] = ds_is_normalized_attr

    if rasterio_data_array:
        rd = rasterio_data_array
    rd = rasterio_data_array or rasterio.open(filename)



    # TODO(#159): Explore ways to capture required metadata using xarray.
    # with rasterio.open(filename) as f:
    datetime_value_ms = None
    try:
        datetime_value_ms = rasterio_data_array.tags()[tif_metadata_for_datetime]
        ds = ds.assign_coords({'time': datetime.datetime.utcfromtimestamp(int(datetime_value_ms) / 1000.0)})
    except KeyError:
        raise RuntimeError(f"Invalid datetime metadata of tif: {tif_metadata_for_datetime}.")
    except ValueError:
        raise RuntimeError(f"Invalid datetime value in tif's metadata: {datetime_value_ms}.")
    ds = ds.expand_dims('time')
    rd.close()

    return ds


def _to_utc_timestring(np_time: np.datetime64) -> str:
    """Turn a numpy datetime64 into UTC timestring."""
    timestamp = float((np_time - np.datetime64(0, 's')) / np.timedelta64(1, 's'))
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')


def _add_is_normalized_attr(ds: xr.Dataset, value: bool) -> xr.Dataset:
    """Adds is_normalized to the attrs of the xarray.Dataset.

    This attribute represents if the dataset is the merged dataset (i.e. created by combining N datasets,
    specifically for normalizing grib's schema) or not.
    """
    ds.attrs['is_normalized'] = value
    return ds


def _is_3d_da(da):
    """Checks whether data array is 3d or not."""
    return len(da.shape) == 3


def __normalize_grib_dataset(filename: str) -> xr.Dataset:
    """Reads a list of datasets and merge them into a single dataset."""
    _data_array_list = []
    list_ds = cfgrib.open_datasets(filename)
    # '<level>_<height>_<attrs['GRIB_stepType']>_<key>'
    for ds in list_ds:
        coords_set = set(ds.coords.keys())
        level_set = coords_set.difference(DEFAULT_COORD_KEYS)
        level = level_set.pop()
        # logger.info(f'{level=}')

        # Now look at what data vars are in each level.
        for key in ds.data_vars.keys():
            # logger.info(f'{key=}')
            da = ds[key]  # The data array
            attrs = da.attrs  # The metadata for this dataset.
            # logger.info(f'{attrs=}')

            # Also figure out the forecast hour for this file.
            forecast_hour = int(da.step.values / np.timedelta64(1, 'h'))

            # We are going to treat the time field as start_time and the
            # valid_time field as the end_time for EE purposes. Also, get the
            # times into UTC timestrings.
            start_time = _to_utc_timestring(da.time.values)
            end_time = _to_utc_timestring(da.valid_time.values)

            attrs['forecast_hour'] = forecast_hour  # Stick the forecast hour in the metadata as well, that's useful.
            attrs['start_time'] = start_time
            attrs['end_time'] = end_time

            no_of_levels = da.shape[0] if _is_3d_da(da) else 1

            # Deal with the randomness that is 3d data interspersed with 2d.
            # For 3d data, we need to extract ds for each value of level.
            for sub_c in range(no_of_levels):
                copied_da = da.copy(deep=True)
                # logger.info(f'{copied_da=}')
                height = copied_da.coords[level].data.flatten()[sub_c]
                # logger.info(f'{height=}')



                # Some heights are super small, but we can't have decimal points
                # in channel names & schema fields for Earth Engine & BigQuery respectively , so mostly cut off the
                # fractional part, unless we are forced to keep it. If so,
                # replace the decimal point with yet another underscore.
                if height >= 10:
                    height_string = f'{height:.0f}'
                else:
                    height_string = f'{height:.2f}'.replace('.', '_')

                channel_name = f'{level}_{height_string}_{attrs["GRIB_stepType"]}_{key}'
                logger.debug('Found channel %s', channel_name)

                # Add the height as a metadata field, that seems useful.
                copied_da.attrs['height'] = height

                copied_da.name = channel_name
                if _is_3d_da(da):
                    copied_da = copied_da.sel({level: height})
                copied_da = copied_da.drop_vars(level)
                _data_array_list.append(copied_da)

    return xr.merge(_data_array_list)


def __open_dataset_file(
  filename: t.Union[str, t.BinaryIO],
  uri: str,
  uri_extension: str,
  disable_grib_schema_normalization: bool,
  open_dataset_kwargs: t.Optional[t.Dict] = None,
  backend_kwargs: t.Optional[t.Dict] = None) -> xr.Dataset:

    # NOTE(bahmandar): checks if file is a cfgrib to be used in for loop
    cfgrib_backend = xr.backends.cfgrib_.CfgribfBackendEntrypoint()
    cfgrib_exists = cfgrib_backend.available
    """Opens the dataset at 'uri' and returns a xarray.Dataset."""

    # NOTE(bahmandar): If someone gave kwargs skip everything else and use that
    if open_dataset_kwargs:
        pass
    # NOTE(bahmandar): For tif and no kwargs use rasterio
    elif uri_extension == '.tif':
        open_dataset_kwargs = {'engine': 'rasterio'}
    elif cfgrib_exists and cfgrib_backend.guess_can_open(uri):
        if not disable_grib_schema_normalization:
            # open with cfgrib.open_datasets
            logger.warning("Assuming grib.")
            logger.info("Normalizing the grib schema, name of the data variables will look like "
                        "'<level>_<height>_<attrs['GRIB_stepType']>_<key>'.")
            # NOTE(bahmandar): cfGrib can't handle file like so if it is file like
            # then read it into bytes. This will be a major memory hit for large
            # grib files
            if isinstance(filename, str):
                return _add_is_normalized_attr(__normalize_grib_dataset(filename), True)
            else:
                return _add_is_normalized_attr(__normalize_grib_dataset(filename.read()), True)
        else:
            # NOTE(bahmandar): If disable_grib_schema_normalization is true and the file
            # is a grib file then open with cfgrib engine
            if cfgrib_backend.available:
                open_dataset_kwargs = {'engine': 'cfgrib'}
                backend_kwargs={'indexpath': ''}

    # NOTE(bahmandar): these can't be none. either filled or empty dicts
    open_dataset_kwargs = open_dataset_kwargs if open_dataset_kwargs is not None else {}
    backend_kwargs = backend_kwargs if backend_kwargs is not None else {}

    # NOTE(bahmandar): Seems like when xarray lazy loads stuff and someone access it
    # what was accessed will stay in memory. (i.e. ds.time[0].values twice in a row will
    # not take twice the time cause cache is True. For our case we need it just once
    # and don't want it to linger in memory so we set this to False
    open_dataset_kwargs['cache'] = False

    # NOTE(bahmandar): Didn't see speed bumps with these but probably needs testing of
    # different scenarios. Rather not use them if not needed
    # open_dataset_kwargs['decode_times'] = False
    # open_dataset_kwargs['decode_timedelta'] = False
    # open_dataset_kwargs['decode_cf'] = False

    # NOTE(bahmandar): Runs twice to match the original function. better to remove if
    # not needed
    for attempt in range(2):
        try:
            return _add_is_normalized_attr(xr.open_dataset(filename, backend_kwargs=backend_kwargs, **open_dataset_kwargs), False)
        # NOTE(bahmandar): saw datasetbuilderror
        except (ValueError, DatasetBuildError) as e:
            if "multiple values for key 'edition'" not in str(e):
                raise
            logger.warning("Assuming grib edition 1.")
            backend_kwargs = {'filter_by_keys': {'edition': 1}, 'indexpath': ''}
            continue


def get_temp_gcs_info(uri: str, temp_gcs_location: str) -> t.Tuple[str, str]:
    temp_gcs_parsed = urlparse(temp_gcs_location)
    zar_name = uri_to_hash_file(uri, '.zarr')
    bucket_name = temp_gcs_parsed.netloc
    prefix = os.path.join(temp_gcs_parsed.path[1:], zar_name)
    return bucket_name, prefix

# NOTE(bahmandar): if vm doesn't have allocated size then there will exist
# no temp directory available
def has_temp_dir():
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            return True
    # NOTE(bahmandar): OSError to catch disk full as well
    except (RuntimeError, FileNotFoundError, OSError):
        return False


def uri_to_filepath(uri: str, temp_dir: str) -> t.Union[str, None]:
    local_filename = uri_to_hash_file(uri)
    return os.path.join(temp_dir, local_filename)


def uri_to_hash_file(uri: str, extension=None) -> str:
    uri_parsed = urlparse(uri)
    if extension is None:
        extension = pathlib.Path(uri_parsed.path).suffix
    local_filename = pathlib.Path(sha256(uri.encode()).hexdigest()[:22]).with_suffix(extension)
    return str(local_filename)

class gcloudException(Exception):
    pass

def log_subprocess_output(pipe):
    for line in iter(pipe.readline, ''): # b'\n'-separated lines
        line_output = line.strip()
        if line_output:
            logger.info(f'TTT {HOSTNAME}: subprocess: {line_output}')
            # NOTE(bahmandar): A hash mismatch didn't put out an exit code != 0
            # this is the reason for this check
            if "ERROR" in line_output:
                raise gcloudException(line_output)


def delete_local_file(uri):
    if has_temp_dir():
        local_file_location = uri_to_filepath(uri, tempfile.gettempdir())
        t = Timer(name='', text='{name}: {:.2f} seconds', logger=logger.info)
        t.start()
        try:
            FileSystems.delete([local_file_location])
            t.name = f'TTT {HOSTNAME}: Deleted Local File: {local_file_location}'
            t.stop()
        except beam.io.filesystem.BeamIOError:
            pass


def delete_local_based_on_time(local_file_location, modification_time):
    if modification_time == os.stat(local_file_location).st_mtime:
        try:
            os.remove(local_file_location)
        except FileNotFoundError:
            pass



def get_system_free_size():
    temp_dir = pathlib.Path(tempfile.gettempdir())
    statvfs = os.statvfs(temp_dir)
    # NOTE(bahmandar): actual disk space used (sparse files will be logical not physical)
    disk_space_available = statvfs.f_frsize * statvfs.f_bavail
    # NOTE(bahmandar): This shows the result from du
    temp_used_artificial = sum(f.stat().st_size for f in temp_dir.glob('**/*') if f.is_file())
    # NOTE(bahmandar): This shows the result from ls
    temp_used_actual = sum(f.stat().st_blocks*512 for f in temp_dir.glob('**/*') if f.is_file())
    return disk_space_available - temp_used_actual + temp_used_artificial


# NOTE(bahmandar): now when dealing with local save it will wait and retry again
# later
@tenacity.retry(
  wait=tenacity.wait_random_exponential(multiplier=0.5, max=60),
  stop=(tenacity.stop_after_delay(30 * 60)), #| tenacity.stop_after_attempt(100)
  before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
  after=tenacity.after_log(logger, logging.INFO),
  retry_error_callback=lambda retry_state: False)
def space_available(source_file_size=None):
    disk_space_available = get_system_free_size()
    if source_file_size * 1.5 > disk_space_available:
        logger.info(f'TTT {HOSTNAME}: no disk space will try again later, disk space: {disk_space_available}, source file: {source_file_size}')
        raise Exception('No Disk Space Left. Still Retrying')
    else:
        logger.info(f'TTT {HOSTNAME}: has disk space will not try again later, disk space: {disk_space_available}, source file: {source_file_size}')
        return True


def get_uri_size(uri):
    with FileSystems.open(uri) as remote_file:
        return get_buffer_size(remote_file)


def copy_with_shutil(uri, dest_file):
    with apache_beam.io.gcsio.GcsIO().open(filename=uri,
                                           read_buffer_size=CUSTOM_READ_BUFFER_SIZE,
                                           mode="rb", mime_type='application/octet-stream') as source_file:
        shutil.copyfileobj(source_file, dest_file, CUSTOM_READ_BUFFER_SIZE)



@tenacity.retry(
  wait=tenacity.wait_random_exponential(multiplier=0.5, max=60),
  stop=(tenacity.stop_after_attempt(2)),
  before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
  after=tenacity.after_log(logger, logging.INFO),
  retry_error_callback=lambda retry_state: 1)
def copy_with_gcloud(uri, dest_file):
    try:
        process = Popen(['gcloud', 'alpha', 'storage', 'cp', uri, dest_file.name], stdout=PIPE, stderr=STDOUT, universal_newlines=True)
        with process.stdout:
            log_subprocess_output(process.stdout)
        exitcode = process.wait() # 0 means success
        return exitcode
    except OSError as e:
        logger.warning(e)
        # yield None
        return 1

@contextlib.contextmanager
def open_local_gcs(uri: str, ignore_existing: bool = False) -> t.ContextManager[t.Union[str, None]]:
    t = Timer(name='', text='{name}: {:.2f} seconds', logger=logger.info)
    t.start()
    if has_temp_dir():
        local_file_location = uri_to_filepath(uri, tempfile.gettempdir())
        if not FileSystems.exists(local_file_location) or ignore_existing:
            with FileSystems.create(local_file_location) as dest_file:
                source_file_size = get_uri_size(uri)
                if not space_available(source_file_size=source_file_size):
                    disk_space_available = get_system_free_size()
                    logger.warning(f'Short on Disk Space - Available: {convert_size(disk_space_available)} File: {convert_size(source_file_size)}')
                    yield None
                else:

                    # NOTE(bahmandar): creates blank file to pre-allocate space
                    # one issue is that gcloud ran out of space while downloading the file
                    dest_file.truncate(source_file_size)
                    try:
                        exit_code = copy_with_gcloud(uri, dest_file)
                        if exit_code != 0:
                            # with FileSystems().open(uri) as source_file:
                            copy_with_shutil(uri, dest_file)
                        dest_file.flush()
                        dest_file.seek(0)

                        t.name = f'TTT {HOSTNAME}: Created Local GCS File: {dest_file.name}'
                        t.stop()
                    except Exception:
                        yield None
                    try:
                        modification_time = time.time()
                        accessed_time = os.stat(local_file_location).st_atime
                        os.utime(local_file_location, (accessed_time, modification_time))
                        yield dest_file.name
                        file_erase = threading.Timer(60, delete_local_based_on_time, (local_file_location, modification_time))
                        file_erase.start()
                    except FileNotFoundError as e:
                        logger.warning(f'{HOSTNAME}: {e} ')
                        yield None
        else:
            with FileSystems.open(local_file_location) as local_file:
                with FileSystems.open(uri) as remote_file:
                    if get_buffer_size(local_file) == get_buffer_size(remote_file):
                        t.name = f'TTT {HOSTNAME}: Reused Local GCS File: {local_file_location}'
                        t.stop()
                        modification_time = time.time()
                        accessed_time = os.stat(local_file_location).st_atime
                        # NOTE(bahmandar): changes modification time. Didn't find
                        # any good way to share this fail between threads and workers
                        # they were re-downloading again which I wanted to stop
                        os.utime(local_file_location, (accessed_time, modification_time))
                        yield local_file_location
                        # NOTE(bahmandar): If no other thread/process picks this up
                        # it will be deleted in 60 seconds
                        file_erase = threading.Timer(60, delete_local_based_on_time, (local_file_location, modification_time))
                        file_erase.start()
                    else:
                        # NOTES(bahmandar): Need to test this out again. UNTESTED
                        t.name = f'TTT {HOSTNAME}: Deleted Corrupted File: {local_file_location}'
                        t.stop()
                        yield open_local_gcs(uri, True)

    else:
        yield None


# TODO(bahmandar) update this if modification time thing works
# NOTE(bahmandar): Focus was on GCS usage but cleaning this up if need is a good
# next step
@contextlib.contextmanager
def open_local(uri: str) -> t.ContextManager[str]:
    """Copy a cloud object (e.g. a netcdf, grib, or tif file) from cloud storage, like GCS, to local file."""
    t = Timer(name='', text='{name}: {:.2f} seconds', logger=logger.info)
    t.start()
    with FileSystems().open(uri) as source_file:
        # with tempfile.TemporaryDirectory() as temp_dir:
        with tempfile.NamedTemporaryFile() as local_file_location:
            # local_file_location = uri_to_filepath(uri, temp_dir)
            with FileSystems().create(local_file_location.name) as dest_file:
                shutil.copyfileobj(source_file, dest_file, DEFAULT_READ_BUFFER_SIZE)
                dest_file.flush()
                dest_file.seek(0)
                t.name = f'TTT {HOSTNAME}: Created Local File: {dest_file.name}'
                t.stop()
                yield dest_file.name


def temp_zarr_exists(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return next(blobs, None) is not None

# NOTE(bahmandar): not currently used, but this was needed cause fsspec would give
# OSError
@tenacity.retry(
  wait=tenacity.wait_random_exponential(multiplier=0.5, max=10),
  stop=(tenacity.stop_after_attempt(3)),
  before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
  after=tenacity.after_log(logger, logging.INFO))
def get_fsspec_mapper(url, check=True):
    return fsspec.get_mapper(url=url, check=False)

# NOTE(bahmandar): Gets subset if possible. returns nothing otherwise
@contextlib.contextmanager
def get_dataset(bucket_name: str,
                prefix: str) -> t.ContextManager[xarray.Dataset]:
    t = Timer(name='', text='{name}: {:.2f} seconds', logger=logger.info)
    t.start()
    if temp_zarr_exists(bucket_name, prefix):
        try:
            # NOTE(bahmandar): both of the options below worked fine. No significant
            # speed difference for the one test
            open_dataset_kwargs = {'cache': False}
            with xr.open_dataset(
              f"gcs://{bucket_name}/{prefix}", **open_dataset_kwargs,
              backend_kwargs={
                "storage_options": {"project": PROJECT, "token": CREDS.token}
              },
              engine="zarr",
            ) as ds:
                t.name = f'TTT {HOSTNAME}: Received Dataset: gs://{bucket_name}/{prefix}'
                t.stop()
                yield ds

            # mapper = get_fsspec_mapper(url=f'gs://{bucket_name}/{prefix}', check=True)
            # with xr.open_zarr(store=mapper) as ds:
            #     t.name = f'TTT {HOSTNAME}: Received Dataset: gs://{bucket_name}/{prefix}'
            #     t.stop()
            #     yield ds
        except GroupNotFoundError as e:
            # logger.warning(type(e).__name__, e.args)
            yield None
    else:
        t.name = f'TTT {HOSTNAME}: Dataset Not Found: gs://{bucket_name}/{prefix}'
        t.stop()
        yield None


# NOTE(bahmandar): saves subset as zarr for future workers to use.
@tenacity.retry(
  wait=tenacity.wait_random_exponential(multiplier=0.5, max=10),
  stop=(tenacity.stop_after_delay(5 * 60) | tenacity.stop_after_attempt(50)), #
  before_sleep=tenacity.before_sleep_log(logger, logging.INFO),
  after=tenacity.after_log(logger, logging.INFO))
def save_dataset(ds: xr.Dataset, bucket_name: str, prefix: str) -> None:
    t = Timer(name=f'TTT {HOSTNAME}: Created Dataset: gs://{bucket_name}/{prefix}', text='{name}: {:.2f} seconds', logger=logger.info)
    t.start()
    mapper = fsspec.get_mapper(url=f'gs://{bucket_name}/{prefix}', create=True)
    ds.to_zarr(store=mapper, mode='w')
    logger.info(f'Dataset: gs://{bucket_name}/{prefix} Created')
    t.stop()


def delete_remote_temp_file(uri, temp_gcs_location):
    if temp_gcs_location:
        bucket_name, prefix = get_temp_gcs_info(uri, temp_gcs_location)
        delete_dataset(bucket_name, prefix)


def delete_remote_temp_files(uris, temp_gcs_location):
    uris_deduped = list(set(uris))
    for uri in uris_deduped:
        if temp_gcs_location:
            bucket_name, prefix = get_temp_gcs_info(uri, temp_gcs_location)
            delete_dataset(bucket_name, prefix)


def delete_local_temp_files(uris):
    uris_deduped = list(set(uris))
    for uri in uris:
        delete_local_file(uri)

# NOTE(bahmandar): This is not needed anymore because we are using tempfile
# if we weren't then we would need to delete manually created files
def delete_dataset(bucket_name: str, prefix: str) -> None:
    t = Timer(name='', text='{name}: {:.2f} seconds', logger=logger.info)
    t.start()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    try:

        blobs = bucket.list_blobs(prefix=prefix)
        # NOTE(bahmandar): HTTPError if you don't turn it into list first
        bucket.delete_blobs(blobs=list(blobs))

        # NOTE(bahmandar): this is for single file and not folders which is what
        # zarr is
        # blob = bucket.blob(prefix)
        # blob.delete()
        t.name = f'TTT {HOSTNAME}: Deleted Dataset: gs://{bucket_name}/{prefix}'
    except NotFound:
        t.name = f'TTT {HOSTNAME}: Nothing to Delete: gs://{bucket_name}/{prefix}'
        pass
    t.stop()


def get_buffer_size(buff: t.BinaryIO) -> int:
    buffer_size = buff.seek(0, io.SEEK_END)
    buff.seek(0)
    return buffer_size


@contextlib.contextmanager
def open_remote_gcs(uri: str) -> t.ContextManager[io.BufferedReader]:
    t = Timer(name='', text='{name}: {:.2f} seconds', logger=logger.info)
    t.start()
    with GcsIO().open(filename=uri, read_buffer_size=CUSTOM_READ_BUFFER_SIZE,  mode="rb", mime_type='application/octet-stream') as file:
        t.name = f'TTT {HOSTNAME}: Opened Remote GCS File: {pathlib.Path(uri).name}'
        t.stop()
        yield file


@contextlib.contextmanager
def open_remote(uri: str) -> t.ContextManager[io.BufferedReader]:
    t = Timer(name='', text='{name}: {:.2f} seconds', logger=logger.info)
    t.start()
    with FileSystems.open(uri, mime_type='application/octet-stream', compression_type=CompressionTypes.AUTO) as file:
        t.name = f'TTT {HOSTNAME}: Opened Remote File: {pathlib.Path(uri).name}'
        t.stop()
        yield file


@contextlib.contextmanager
def open_dataset(uri,
                 open_dataset_kwargs: t.Optional[t.Dict] = None,
                 disable_in_memory_copy: bool = False,
                 disable_grib_schema_normalization: bool = False,
                 tif_metadata_for_datetime: t.Optional[str] = None,
                 area: t.Tuple[int, int, int, int] = None,
                 variables: t.List[str] = None,
                 enable_local_save: bool = None,
                 temp_gcs_location: str = None,
                 run_rasterio: bool = False,
                 backend_kwargs: t.Optional[t.Dict] = None
                 ) -> t.ContextManager[xr.Dataset]:
    """Open the dataset at 'uri' and return a xarray.Dataset."""

    try:
        # NOTE(bahmandar): if a temp gcs location was given and the zarr file
        # exists then use that
        with contextlib.ExitStack() as stack:
            xr_dataset = None
            if temp_gcs_location:
                bucket_name, prefix = get_temp_gcs_info(uri, temp_gcs_location)
                xr_dataset = stack.enter_context(get_dataset(bucket_name, prefix))
            if xr_dataset is not None:
                yield xr_dataset
            else:
                # NOTE(bahmandar): check if scipy and prefer local save
                # but only if local save was never set to begin with
                scipy_backend = xr.backends.scipy_.ScipyBackendEntrypoint()
                with FileSystems.open(uri) as f:
                    if scipy_backend.available and scipy_backend.guess_can_open(f):
                        if enable_local_save is None:
                            logger.info('Local save is preferred for this file type')
                            enable_local_save = True

                # NOTE(bahmandar): try to local gcs save and if it fails then
                # resort to remote open
                if enable_local_save:
                    if FileSystems.get_scheme(uri) == 'gs':
                        uri_buffer = stack.enter_context(open_local_gcs(uri))
                        if uri_buffer is None:
                            uri_buffer = stack.enter_context(open_remote_gcs(uri))
                    else:
                        # NOTE(bahmandar): This doesn't have a check if the system
                        # is full
                        uri_buffer = stack.enter_context(open_local(uri))
                # NOTE(bahmandar): open gcs with a different buffer size
                # but the following two could be merged into one at some point
                elif FileSystems.get_scheme(uri) == 'gs':
                    uri_buffer = stack.enter_context(open_remote_gcs(uri))
                else:
                    uri_buffer = stack.enter_context(open_remote(uri))

                _, uri_extension = os.path.splitext(uri)

                t = Timer(name='', text='{name}: {:.2f} seconds', logger=logger.info)
                t.start()
                xr_dataset: xr.Dataset = __open_dataset_file(uri_buffer, uri, uri_extension, disable_grib_schema_normalization, open_dataset_kwargs, backend_kwargs)
                # NOTE(bahmandar): original size before filtering
                original_size = xr_dataset.nbytes
                t.name = f'TTT {HOSTNAME}: Opened Dataset: {pathlib.Path(uri).name}, {convert_size(xr_dataset.nbytes)}'
                t.stop()

                beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()

                if variables:
                    xr_dataset: xr.Dataset = _only_target_vars(xr_dataset, variables)

                if area:
                    n, w, s, e = area
                    # print([coord.name for coord in (x, y, t)])
                    if 'longitude' in xr_dataset:
                        xr_dataset = xr_dataset.sel(latitude=slice(n, s), longitude=slice(w, e))
                    elif 'lon' in xr_dataset:
                        xr_dataset = xr_dataset.sel(lat=slice(n, s), lon=slice(w, e))
                    logger.info(f'Data filtered by area, size: {convert_size(xr_dataset.nbytes)}')

                # NOTE(bahmandar): This is NOT TESTED and this will have to load everything into
                # memory or have it available locally
                if run_rasterio:
                    with rasterio.io.MemoryFile(uri_buffer) as memfile:
                        with memfile.open() as dataset:
                            data_array = dataset.read()
                            dtype, crs, transform = (data_array.profile.get(key) for key in ['dtype', 'crs', 'transform'])
                            xr_dataset.attrs.update({'dtype': dtype, 'crs': crs, 'transform': transform})
                            # NOTE(bahmandar): NEEDS TO BE TESTED. This is inside
                            # rasterio cause that is that is used inside _preprocess_tif
                            if uri_extension == '.tif':
                                xr_dataset = _preprocess_tif(xr_dataset, None, tif_metadata_for_datetime, data_array)

                current_size = xr_dataset.nbytes
                if current_size < original_size * 0.25 and temp_gcs_location and enable_local_save:
                    bucket_name, prefix = get_temp_gcs_info(uri, temp_gcs_location)
                    save_dataset(xr_dataset, bucket_name, prefix)
                    # uri_buffer = stack.enter_context(get_dataset(bucket_name, prefix))
                    xr_dataset = stack.enter_context(get_dataset(bucket_name, prefix))
                    # if uri_buffer is not None:
                    #     xr_dataset: xr.Dataset = __open_dataset_file(uri_buffer, f'gs://{bucket_name}/{prefix}', '.zarr', True, None, None)
                    if xr_dataset is not None:
                        delete_local_file(uri)

                # NOTE(bahmandar): Permenantly not used cause it will have signifcant memory issues
                # with large files (i.e. greater than 2GiB).
                disable_in_memory_copy = True
                if not disable_in_memory_copy:
                    t = Timer(name=f'TTT {HOSTNAME}: Loading Dataset: {pathlib.Path(uri).name}', text='{name}: {:.2f} seconds', logger=logger.info)
                    t.start()
                    # NOTE(bahmandar): compute seemed to the trick for loading it
                    # and I'm not sure how will _make_grib_dataset_inmem will play with a
                    # lazily loaded dask array. A way to check that is by lazy opening
                    # a zarr and testing it out
                    # xr_dataset = _make_grib_dataset_inmem(xr_dataset)
                    xr_dataset = xr_dataset.compute()
                    t.stop()
                yield xr_dataset
    except Exception as e:
        beam.metrics.Metrics.counter('Failure', 'ReadNetcdfData').inc()
        logger.error(f'Unable to open file {uri!r}: {e}')
        raise
