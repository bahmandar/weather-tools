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

import xarray.backends.cfgrib_
from pyproj import Transformer
import numpy as np
import datetime
from rasterio.io import MemoryFile
import apache_beam as beam
import rasterio
import xarray as xr
import cfgrib
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import DEFAULT_READ_BUFFER_SIZE
from osgeo import gdal
from urllib.parse import urlparse
import pathlib
from apache_beam.io.fileio import ReadableFile
from apache_beam.io.filesystem import CompressionTypes
from cfgrib.dataset import DatasetBuildError

from hashlib import sha256
from .util import (
    DEFAULT_COORD_KEYS,
    _only_target_vars,
    convert_size
)
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


def _preprocess_tif(ds: xr.Dataset, filename: str, tif_metadata_for_datetime: str) -> xr.Dataset:
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

    # TODO(#159): Explore ways to capture required metadata using xarray.
    with rasterio.open(filename) as f:
        datetime_value_ms = None
        try:
            datetime_value_ms = f.tags()[tif_metadata_for_datetime]
            ds = ds.assign_coords({'time': datetime.datetime.utcfromtimestamp(int(datetime_value_ms) / 1000.0)})
        except KeyError:
            raise RuntimeError(f"Invalid datetime metadata of tif: {tif_metadata_for_datetime}.")
        except ValueError:
            raise RuntimeError(f"Invalid datetime value in tif's metadata: {datetime_value_ms}.")
        ds = ds.expand_dims('time')

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



def __open_dataset_file_new(
    filename: t.Union[str, t.BinaryIO],
    uri: str,
    uri_extension: str,
    disable_grib_schema_normalization: bool,
    open_dataset_kwargs: t.Optional[t.Dict] = None) -> xr.Dataset:
    backend_kwargs = {}

    cfgrib_backend = xarray.backends.cfgrib_.CfgribfBackendEntrypoint()
    cfgrib_exists = cfgrib_backend.available
    """Opens the dataset at 'uri' and returns a xarray.Dataset."""

    # If URI extension is .tif, try opening file by specifying engine="rasterio".
    if open_dataset_kwargs is not None:
        pass
    elif uri_extension == '.tif':
        # no kwargs and uri tif
        open_dataset_kwargs = {'engine': 'rasterio'}
    elif cfgrib_backend.guess_can_open(uri):
        # Its likely grib file
        if not disable_grib_schema_normalization:
            # open with cfgrib.open_datasets
            logger.warning("Assuming grib.")
            logger.info("Normalizing the grib schema, name of the data variables will look like "
                        "'<level>_<height>_<attrs['GRIB_stepType']>_<key>'.")
            return _add_is_normalized_attr(__normalize_grib_dataset(filename), True)
        else:
            if cfgrib_backend.available:
                # Trying with explicit engine for cfgrib.
                open_dataset_kwargs = {'engine': 'cfgrib'}
                backend_kwargs={'indexpath': ''}


    open_dataset_kwargs = open_dataset_kwargs if open_dataset_kwargs is not None else {}
    backend_kwargs = backend_kwargs if backend_kwargs is not None else {}

    open_dataset_kwargs['cache'] = False
    for attempt in range(2):
        try:
            return _add_is_normalized_attr(xr.open_dataset(filename, backend_kwargs=backend_kwargs, **open_dataset_kwargs), False)

        # Saw datasetbuilderror
        except (ValueError, DatasetBuildError) as e:
            if "multiple values for key 'edition'" not in str(e):
                raise
            logger.warning("Assuming grib edition 1.")
            backend_kwargs = {'filter_by_keys': {'edition': 1}, 'indexpath': ''}
            continue








def __open_dataset_file(filename: str,
                        uri_extension: str,
                        disable_grib_schema_normalization: bool,
                        open_dataset_kwargs: t.Optional[t.Dict] = None) -> xr.Dataset:
    """Opens the dataset at 'uri' and returns a xarray.Dataset."""
    if open_dataset_kwargs:
        return _add_is_normalized_attr(xr.open_dataset(filename, **open_dataset_kwargs), False)

    # If URI extension is .tif, try opening file by specifying engine="rasterio".
    if uri_extension == '.tif':
        return _add_is_normalized_attr(xr.open_dataset(filename, engine='rasterio'), False)

    # If no open kwargs are available and URI extension is other than tif, make educated guesses about the dataset.
    try:
        return _add_is_normalized_attr(xr.open_dataset(filename), False)
    except ValueError as e:
        e_str = str(e)
        if not ("Consider explicitly selecting one of the installed engines" in e_str and "cfgrib" in e_str):
            raise

    if not disable_grib_schema_normalization:
        logger.warning("Assuming grib.")
        logger.info("Normalizing the grib schema, name of the data variables will look like "
                    "'<level>_<height>_<attrs['GRIB_stepType']>_<key>'.")
        return _add_is_normalized_attr(__normalize_grib_dataset(filename), True)

    # Trying with explicit engine for cfgrib.
    try:
        return _add_is_normalized_attr(
            xr.open_dataset(filename, engine='cfgrib', backend_kwargs={'indexpath': ''}),
            False)
    except ValueError as e:
        if "multiple values for key 'edition'" not in str(e):
            raise
    logger.warning("Assuming grib edition 1.")
    # Try with edition 1
    # Note: picking edition 1 for now as it seems to get the most data/variables for ECMWF realtime data.
    return _add_is_normalized_attr(
        xr.open_dataset(filename, engine='cfgrib', backend_kwargs={'filter_by_keys': {'edition': 1}, 'indexpath': ''}),
        False)


def uri_to_filepath(uri: str) -> str:
    uri_parsed = urlparse(uri)
    extension = pathlib.Path(uri_parsed.path).suffix
    local_filename = pathlib.Path(sha256(uri.encode()).hexdigest()[:22]).with_suffix(extension)
    return str(tempfile.gettempdir() / local_filename)


def get_local(uri: str) -> str:
    """Copy a cloud object (e.g. a netcdf, grib, or tif file) from cloud storage, like GCS, to local file."""

    #TODO need a method to check file integrity locally
    local_file_location = uri_to_filepath(uri)
    print(f'{local_file_location=}')


    if not FileSystems.exists(local_file_location):
        with FileSystems.open(uri) as remote_file:
            with FileSystems.create(local_file_location) as local_file:
                shutil.copyfileobj(remote_file, local_file, DEFAULT_READ_BUFFER_SIZE)
    else:
        print(f'checksum={FileSystems.checksum(local_file_location)}')
    return local_file_location


def dump(obj):
    for attr in dir(obj):
        try:
            print('obj.%s = %r' % (attr, getattr(obj, attr)))
        except:
            pass

@contextlib.contextmanager
def open_local(uri: str) -> t.Iterator[str]:
    """Copy a cloud object (e.g. a netcdf, grib, or tif file) from cloud storage, like GCS, to local file."""
    with FileSystems().open(uri) as source_file:
        with tempfile.NamedTemporaryFile() as dest_file:
            shutil.copyfileobj(source_file, dest_file, DEFAULT_READ_BUFFER_SIZE)
            dest_file.flush()
            dest_file.seek(0)
            yield dest_file.name


@contextlib.contextmanager
def open_dataset_modified(element: t.Union[ReadableFile,None],
                          uri,
                          open_dataset_kwargs: t.Optional[t.Dict] = None,
                          disable_in_memory_copy: bool = False,
                          disable_grib_schema_normalization: bool = False,
                          tif_metadata_for_datetime: t.Optional[str] = None,
                          disable_local_save: bool = False,
                          area: t.Tuple[int, int, int, int] = None,
                          variables: t.List[str] = None) -> t.Iterator[xr.Dataset]:
    """Open the dataset at 'uri' and return a xarray.Dataset."""
    try:

        logger.info(f'Opening: {uri}')
        # print(rasterio.drivers.raster_driver_extensions())

        # By copying the file locally, xarray can open it much faster via an in-memory copy.
        _, uri_extension = os.path.splitext(uri)

        # This if/else needs cleanup
        if disable_local_save:
            rasterio_path = os.path.join(os.sep, 'vsimem', uri_to_filepath(uri).lstrip("/"))
            if not element:
                filesystem = FileSystems.get_filesystem(uri)
                element: ReadableFile = ReadableFile(filesystem.metadata(uri))
        else:
            rasterio_path = get_local(uri)
            print(f'{rasterio_path=}')
            filesystem = FileSystems.get_filesystem(rasterio_path)
            element: ReadableFile = ReadableFile(filesystem.metadata(rasterio_path))

        with element.open(mime_type='application/octet-stream', compression_type=CompressionTypes.AUTO) as uri_buffer:
            logger.info(f'Element Open: {uri}')
            gdal.UseExceptions()
            if disable_local_save:
                try:
                    dataset = gdal.Open(rasterio_path, gdal.GA_ReadOnly)
                except RuntimeError:
                    print('create vsimem')
                    gdal.FileFromMemBuffer(rasterio_path, uri_buffer.read())
                    uri_buffer.seek(0)
                finally:
                    dataset = None



            # open_dataset_kwargs['filter_by_keys'] = {'typeOfLevel': 'surface'}


            disable_grib_schema_normalization = True
            xr_dataset: xr.Dataset = __open_dataset_file_new(rasterio_path, uri, uri_extension, disable_grib_schema_normalization, open_dataset_kwargs)

            logger.info(f'XR Dataset Loaded: {uri}')


            if uri_extension == '.tif':
                xr_dataset = _preprocess_tif(xr_dataset, rasterio_path, tif_metadata_for_datetime)

            # if not disable_in_memory_copy:
            #     xr_dataset = _make_grib_dataset_inmem(xr_dataset)


            # Extracting dtype, crs and transform from the dataset & storing them as attributes.
            with rasterio.open(rasterio_path, 'r') as f:
                # print(f.dtypes)
                # print(f.nodatavals)
                dtype, crs, transform = (f.profile.get(key) for key in ['dtype', 'crs', 'transform'])
                xr_dataset.attrs.update({'dtype': dtype, 'crs': crs, 'transform': transform})


            logger.info(f'opened dataset size: {convert_size(xr_dataset.nbytes)}')
            beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()

            xr_dataset: xr.Dataset = _only_target_vars(xr_dataset, variables)

            if area:
                n, w, s, e = area
                xr_dataset = xr_dataset.sel(latitude=slice(n, s), longitude=slice(w, e))
                logger.info(f'Data filtered by area, size: {convert_size(xr_dataset.nbytes)}')
        yield xr_dataset
    except Exception as e:
        beam.metrics.Metrics.counter('Failure', 'ReadNetcdfData').inc()
        logger.error(f'Unable to open file {uri!r}: {e}')
        raise


@contextlib.contextmanager
def open_dataset(uri: str,
                 open_dataset_kwargs: t.Optional[t.Dict] = None,
                 disable_in_memory_copy: bool = False,
                 disable_grib_schema_normalization: bool = False,
                 tif_metadata_for_datetime: t.Optional[str] = None) -> t.Iterator[xr.Dataset]:
    """Open the dataset at 'uri' and return a xarray.Dataset."""
    try:
        # By copying the file locally, xarray can open it much faster via an in-memory copy.
        with open_local(uri) as local_path:
            _, uri_extension = os.path.splitext(uri)
            xr_dataset: xr.Dataset = __open_dataset_file(local_path,
                                                         uri_extension,
                                                         disable_grib_schema_normalization,
                                                         open_dataset_kwargs)

            if uri_extension == '.tif':
                xr_dataset = _preprocess_tif(xr_dataset, local_path, tif_metadata_for_datetime)

            if not disable_in_memory_copy:
                xr_dataset = _make_grib_dataset_inmem(xr_dataset)

            # Extracting dtype, crs and transform from the dataset & storing them as attributes.
            with rasterio.open(local_path, 'r') as f:
                dtype, crs, transform = (f.profile.get(key) for key in ['dtype', 'crs', 'transform'])
                xr_dataset.attrs.update({'dtype': dtype, 'crs': crs, 'transform': transform})

            logger.info(f'opened dataset size: {xr_dataset.nbytes}')

            beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()
            yield xr_dataset
    except Exception as e:
        beam.metrics.Metrics.counter('Failure', 'ReadNetcdfData').inc()
        logger.error(f'Unable to open file {uri!r}: {e}')
        raise
