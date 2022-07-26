from apache_beam import typehints as tp
import typing as t
import apache_beam as beam
from apache_beam.io.fileio import ReadableFile
import datetime
from google.cloud import bigquery
from apache_beam.transforms import window
from apache_beam.io.restriction_trackers import OffsetRange
from .sinks import open_dataset_modified
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.coders.coders import VarIntCoder
from xarray.core.utils import ensure_us_time_resolution
import pathlib
import json
import geojson
import xarray as xr
import math
import numpy as np
import logging
from .util import (
  to_json_serializable_type,
  get_iterator_minimal

)
import dataclasses

DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()
DATA_IMPORT_TIME_COLUMN = 'data_import_time'
DATA_URI_COLUMN = 'data_uri'
DATA_FIRST_STEP = 'data_first_step'
GEO_POINT_COLUMN = 'geometry'
GEO_TYPE = 'type'
LATITUDE_RANGE = (-90, 90)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_total_count_to_process(ds):
  total_count = 0
  for v in ds.keys():
    total_count += math.prod(ds[v].shape)

  # print(f'{total_count=}')
  # print(f'{type(total_count)=}')

  return total_count


def fetch_geo_point(lat: float, long: float) -> str:
  """Calculates a geography point from an input latitude and longitude."""
  if lat > LATITUDE_RANGE[1] or lat < LATITUDE_RANGE[0]:
    raise ValueError(f"Invalid latitude value '{lat}'")

  long = ((long + 180) % 360) - 180
  point = json.dumps(geojson.Point((long, lat)))
  return point

class FileOutput(t.NamedTuple):
  readable_file: ReadableFile
  uri: str
  num_of_elements: int
  area: t.Tuple[int, int, int, int]
  xarray_open_dataset_kwargs: t.Dict
  disable_in_memory_copy: bool
  tif_metadata_for_datetime: t.Optional[str]
  disable_grib_schema_normalization: bool
  disable_local_save: bool
  coordinate_chunk_size: int
  dry_run: bool
  schema: t.List[bigquery.SchemaField]
  table: bigquery.Table = None
  variables: t.List[str] = None
  import_time: t.Optional[datetime.datetime] = DEFAULT_IMPORT_TIME

def map_dtype_to_sql_type(da: xr.DataArray) -> str:
  """Maps a np.dtype to a suitable BigQuery column type."""
  var_type = da.dtype
  #this doesnt cover the case that item 1 is none, nan or null

  if da.data.size == 1:
    da_item = da.values.item()
  else:
    da_item = da.values.item(1)

  if var_type in {np.dtype('float64'), np.dtype('float32'), np.dtype('timedelta64[ns]')}:
    return 'FLOAT64'
  elif var_type in {np.dtype('<M8[ns]')}:
    return 'TIMESTAMP'
  elif var_type in {np.dtype('int8'), np.dtype('int16'), np.dtype('int32'), np.dtype('int64')}:
    return 'INT64'
  elif type(da_item) == str:
    return 'STRING'
  elif var_type in {np.dtype(object)}:
    if hasattr(da_item, 'isoformat'):
      return 'DATETIME'
  raise ValueError(f"Unknown mapping from '{var_type}' to SQL type")


def dataset_to_table_schema(ds: xr.Dataset) -> t.List[bigquery.SchemaField]:
  """Returns a BigQuery table schema able to store the data in 'ds'."""
  # Get the columns and data types for all variables in the dataframe
  columns = [
      (str(col), map_dtype_to_sql_type(ds[col]))
      for col in ds.variables.keys() if ds[col].size != 0
  ]

  return to_table_schema(columns)

def to_table_schema(columns: t.List[t.Tuple[str, str]]) -> t.List[bigquery.SchemaField]:
  # Fields are all Nullable because data may have NANs. We treat these as null.
  fields = [
      bigquery.SchemaField(column, var_type, mode='NULLABLE')
      for column, var_type in columns
  ]

  # Add an extra columns for recording import metadata.
  fields.append(bigquery.SchemaField(DATA_IMPORT_TIME_COLUMN, 'TIMESTAMP', mode='NULLABLE'))
  fields.append(bigquery.SchemaField(DATA_URI_COLUMN, 'STRING', mode='NULLABLE'))
  # fields.append(bigquery.SchemaField(DATA_FIRST_STEP, 'TIMESTAMP', mode='NULLABLE'))
  fields.append(bigquery.SchemaField(GEO_POINT_COLUMN, 'GEOGRAPHY', mode='NULLABLE'))

  return fields


class FanOut(beam.DoFn):
  def process(self, element: t.List[FileOutput]) -> t.Iterable[FileOutput]:
    for item in element:
      yield item

class AddIndex(beam.DoFn):
  """DoFn that will add indexes to pcollection increasing by one.

  Yields:
    Tuple of the index and element
  """
  INDEX_FILE = ReadModifyWriteStateSpec(name='file_num', coder=VarIntCoder())

  def process(self,
              element: tp.KV[int, tp.Any],
              index_iterator=beam.DoFn.StateParam(INDEX_FILE)
              ) -> tp.Iterable[tp.KV[int, t.Any]]:
    current_index = index_iterator.read() or 1
    yield (current_index, element[1])
    index_iterator.write(current_index + 1)

class WindowPerFile(beam.PTransform):

  def expand(self, pcoll):
    return (
        pcoll
        | 'Add Constant Key' >> beam.Map(lambda v: (0, v))
        | 'Add Indexes' >> beam.ParDo(AddIndex())
        | 'Add Timestamps' >> beam.Map(
        lambda kv: window.TimestampedValue(kv, kv[0]))
        | 'Add Session Windows' >> beam.WindowInto(window.Sessions(1))
        | 'Remove Index Key' >> beam.Values()
    )

@dataclasses.dataclass
class PrepareFiles(beam.DoFn):
  """TBD.

  Yields:
    TBD
  """
  variables: t.List[str] = None
  area: t.Tuple[int, int, int, int] = None
  import_time: t.Optional[datetime.datetime] = None
  xarray_open_dataset_kwargs: t.Dict = None
  disable_in_memory_copy: bool = None
  tif_metadata_for_datetime: t.Optional[str] = None
  disable_grib_schema_normalization: bool = None
  infer_schema: bool = None
  coordinate_chunk_size: int = 20 #10_000
  disable_local_save: bool = False
  dry_run: bool = True

  def process(self, element: ReadableFile) -> tp.Iterable[tp.KV[int, FileOutput]]:
    with open_dataset_modified(element,
                               element.metadata.path,
                               self.xarray_open_dataset_kwargs,
                               self.disable_in_memory_copy,
                               self.disable_grib_schema_normalization,
                               self.tif_metadata_for_datetime,
                               self.disable_local_save,
                               self.area,
                               self.variables
                               ) as ds:
      logger.info(f'Preparing File: {element.metadata.path}')
      fields = {}

      fields['readable_file'] = element
      fields['num_of_elements'] = get_total_count_to_process(ds)
      fields['uri'] = element.metadata.path
      fields.update({k: dataclasses.asdict(self)[k] for k in FileOutput._fields if k in dataclasses.asdict(self)})


      if self.variables and not self.infer_schema and not ds.attrs['is_normalized']:
        logger.info('Creating schema from input variables.')
        fields['schema'] = to_table_schema(
            [('latitude', 'FLOAT64'), ('longitude', 'FLOAT64'), ('time', 'TIMESTAMP')] +
            [(var, 'FLOAT64') for var in self.variables]
        )
      else:
        logger.info('Inferring schema from data.')
        # ds: xr.Dataset = _only_target_vars(open_ds, self.variables)
        fields['schema'] = dataset_to_table_schema(ds)

      yield 0, FileOutput(**fields)

class ProcessXArray(beam.DoFn, beam.transforms.core.RestrictionProvider):
  """Splittable DoFn that will process tar files.

  Yields:
    generator of TarOutput objects.
  """

  def __init__(self):

    super().__init__()

  def initial_restriction(self, element: FileOutput):

    return OffsetRange(0, element.num_of_elements)

  def create_tracker(self, restriction):
    return beam.io.restriction_trackers.OffsetRestrictionTracker(restriction)

  def split(self, element: FileOutput, restriction):
    i = restriction.start
    # element.coordinate_chunk_size
    split_size = element.coordinate_chunk_size
    # split_size = 49032000
    while i < restriction.stop - split_size:
      yield OffsetRange(i, i + split_size)
      i += split_size
    yield OffsetRange(i, restriction.stop)

  def restriction_size(self, element: FileOutput, restriction):
    return restriction.size()

  def process(self, element: FileOutput,
              tracker=beam.DoFn.RestrictionParam()):
    # with element.open() as file:

    with open_dataset_modified(element.readable_file,
                               element.uri,
                               element.xarray_open_dataset_kwargs,
                               element.disable_in_memory_copy,
                               element.disable_grib_schema_normalization,
                               element.tif_metadata_for_datetime,
                               element.disable_local_save,
                               element.area,
                               element.variables
                               ) as ds:
      current_position = tracker.current_restriction().start
      file_name = pathlib.Path(element.uri).stem
      trying = True
      while tracker.try_claim(current_position):
        end = tracker.current_restriction().stop

        for idx, item in enumerate(get_iterator_minimal(ds, current_position, end), start=1):

          # print(idx, item)
          variable = item[0]
          position = item[1]
          row_ds = ds[variable][position]

          # print(f'{ds[variable][position].data=}')



          pcoll_key = []
          row = {variable: to_json_serializable_type(ensure_us_time_resolution(row_ds.data[()]))}
          coords = {}

          for key, value in row_ds.coords.items():
            coord_value = to_json_serializable_type(ensure_us_time_resolution(value.data[()]))
            # row[key] = coord_value
            pcoll_key.append({key: coord_value})
            coords[key] = coord_value
            # pcoll_key.append(coord_value)
            # Re-calculate import time for streaming extractions.
          if not element.import_time:
            import_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
          else:
            import_time = element.import_time
          pcoll_key.append({DATA_IMPORT_TIME_COLUMN: import_time})
          pcoll_key.append({DATA_URI_COLUMN: element.uri})
          # pcoll_key[DATA_IMPORT_TIME_COLUMN] = element.import_time
          # pcoll_key[DATA_URI_COLUMN] = element.uri

          # row[DATA_FIRST_STEP] = first_time_step

          # pcoll_key.append({GEO_TYPE: 'Feature'})
          if 'latitude' in coords and 'longitude' in coords:
            pcoll_key.append({GEO_POINT_COLUMN: fetch_geo_point(coords['latitude'], coords['longitude'])})
          # pcoll_key[GEO_TYPE] = 'Feature'
          # pcoll_key[GEO_POINT_COLUMN] = fetch_geo_point(pcoll_key['latitude'], pcoll_key['longitude'])
          # print(f"{tuple(pcoll_key)=}")
          # print(json.dumps(row,indent=2))
          # print(pcoll_key)
          # print(row)
          yield pcoll_key, row

          if (current_position + idx) >= end or not tracker.try_claim(current_position + idx):
            break

          # might not be needed. good to run with and without this statement for comparison

        current_position += end