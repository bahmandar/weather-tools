

import numpy
from apache_beam import typehints as tp
import typing as t
import apache_beam as beam
import datetime
from google.cloud import bigquery
from apache_beam.io.gcp.internal.clients import bigquery as bigquery_beam
from apache_beam.transforms import window
from apache_beam.io.restriction_trackers import OffsetRange


from .sinks import (
    open_dataset,
    delete_remote_temp_files,
    delete_local_temp_files
)

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
from apache_beam import pvalue

from codetiming import Timer
from .util import (
    to_json_serializable_type,
    get_iterator_minimal
)

import dataclasses
import socket
HOSTNAME = socket.gethostname()

DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()
DATA_IMPORT_TIME_COLUMN = 'data_import_time'
DATA_URI_COLUMN = 'data_uri'
DATA_FIRST_STEP = 'data_first_step'
GEO_POINT_COLUMN = 'geometry'
GEO_TYPE = 'type'
LATITUDE_RANGE = (-90, 90)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# def get_total_count_to_process(ds):
#   total_count = 0
#   for v in ds.keys():
#     total_count += math.prod(ds[v].shape)
#
#   # print(f'{total_count=}')
#   # print(f'{type(total_count)=}')
#
#   return total_count

#
# def get_total_count_to_process(ds):
#   total_count = 0
#
#   for v in ds.keys():
#     total_count += math.prod(ds[v].shape)
#
#   # print(f'{total_count=}')
#   # print(f'{type(total_count)=}')
#
#   return total_count

def fetch_geo_point(lat: float, long: float) -> str:
    """Calculates a geography point from an input latitude and longitude."""
    if lat > LATITUDE_RANGE[1] or lat < LATITUDE_RANGE[0]:
        raise ValueError(f"Invalid latitude value '{lat}'")

    long = ((long + 180) % 360) - 180
    point = json.dumps(geojson.Point((long, lat)))
    return point

class FileOutput(t.NamedTuple):
    uri: str
    num_of_elements: int
    area: t.Tuple[int, int, int, int]
    xarray_open_dataset_kwargs: t.Dict
    disable_in_memory_copy: bool
    tif_metadata_for_datetime: t.Optional[str]
    disable_grib_schema_normalization: bool
    coordinate_chunk_size: int
    dry_run: bool
    schema: t.List[bigquery_beam.TableFieldSchema]
    table: bigquery.Table = None
    variables: t.List[str] = None
    import_time: t.Optional[datetime.datetime] = DEFAULT_IMPORT_TIME
    temp_gcs_location: str = None
    enable_local_save: bool = None
    attrs: dict = None

def map_dtype_to_sql_type(var_type: numpy.dtype, da_item: t.Any) -> str:
    """Maps a np.dtype to a suitable BigQuery column type."""
    #this doesnt cover the case that item 1 is none, nan or null

    if var_type in {np.dtype('float64'), np.dtype('float32'), np.dtype('timedelta64[ns]')}:
        return 'FLOAT64'
    elif var_type in {np.dtype('<M8[ns]')}:
        return 'TIMESTAMP'
    elif var_type in {np.dtype('int8'), np.dtype('int16'), np.dtype('int32'), np.dtype('int64'), np.dtype('uint8')}:
        return 'INT64'
    elif type(da_item) == str:
        return 'STRING'
    # NOTE(bahmandar): rasm.nc issue
    elif var_type in {np.dtype(object)}:
        if hasattr(da_item, 'isoformat'):
            return 'DATETIME'
    raise ValueError(f"Unknown mapping from '{var_type}' to SQL type")


@Timer(name=f'TTT {HOSTNAME}: Inferred Schema From Data', text='{name}: {:.2f} seconds', logger=logger.info)
def dataset_to_table_schema(ds: xr.Dataset) -> t.List[bigquery_beam.TableFieldSchema]:
    """Returns a BigQuery table schema able to store the data in 'ds'."""
    # Get the columns and data types for all variables in the dataframe

    # NOTE(bahmandar): super slow cause it was loading xarray variable entirely
    # columns = [
    #     (str(col), map_dtype_to_sql_type(ds[col]))
    #     for col in ds.variables.keys() if ds[col].size != 0
    # ]

    columns = []
    for col in ds.variables.keys():

        if ds[col].size != 0:
            da_item = ds[col][tuple(0 for _ in range(len(ds[col].shape)))]
            columns.append((str(col), map_dtype_to_sql_type(ds[col].dtype, da_item)))
    return to_table_schema(columns)

# NOTE(bahmandar): Is FIRST_STEP needed?
def to_table_schema(columns: t.List[t.Tuple[str, str]]) -> t.List[bigquery_beam.TableFieldSchema]:
    # Fields are all Nullable because data may have NANs. We treat these as null.
    fields = [
        bigquery_beam.TableFieldSchema(name=column, type=var_type, mode='NULLABLE')
        for column, var_type in columns
    ]
    # Add an extra columns for recording import metadata.
    fields.append(bigquery_beam.TableFieldSchema(name=DATA_IMPORT_TIME_COLUMN, type='TIMESTAMP', mode='NULLABLE'))
    fields.append(bigquery_beam.TableFieldSchema(name=DATA_URI_COLUMN, type='STRING', mode='NULLABLE'))
    # fields.append(bigquery.SchemaFieldbigquery_beam.TableFieldSchema(name=DATA_FIRST_STEP, type='TIMESTAMP', mode='NULLABLE'))
    fields.append(bigquery_beam.TableFieldSchema(name=GEO_POINT_COLUMN, type='GEOGRAPHY', mode='NULLABLE'))
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
      FileOutput Class Objects
    """

    def setup(self):
        self.uris = []
        super().setup()

    temp_gcs_location: str = None
    variables: t.List[str] = None
    area: t.Tuple[int, int, int, int] = None
    import_time: t.Optional[datetime.datetime] = None
    xarray_open_dataset_kwargs: t.Dict = None
    disable_in_memory_copy: bool = None
    tif_metadata_for_datetime: t.Optional[str] = None
    disable_grib_schema_normalization: bool = None
    infer_schema: bool = None
    coordinate_chunk_size: int = 1_000_000
    dry_run: bool = False
    enable_local_save: bool = None
    uris: t.List[str] = None


    #
    # def delete_temp_files(self):
    #     if self.uris:
    #         uris = list(set(self.uris))
    #         for uri in uris:
    #             if self.temp_gcs_location:
    #                 # bucket_name, prefix = get_temp_gcs_info(uri, self.temp_gcs_location)
    #                 # delete_dataset(bucket_name, prefix)
    #                 # NOTE(bahmandar): In prepare we only want to delete any local files and
    #                 # leave the temp gcs files for processing later
    #                 delete_local_file(uri)
    #         self.uris = []
    #
    # # NOTE(bahmandar): This will run if job is "stopped"
    def teardown(self):
        delete_local_temp_files(self.uris)
        super().teardown()
    #
    def finish_bundle(self):
        delete_local_temp_files(self.uris)
        super().finish_bundle()

    def process(self, uri: str) -> tp.Iterable[FileOutput]:
        fields = {}
        self.uris.append(uri)
        with open_dataset(uri,
                                   self.xarray_open_dataset_kwargs,
                                   self.disable_in_memory_copy,
                                   self.disable_grib_schema_normalization,
                                   self.tif_metadata_for_datetime,
                                   self.area,
                                   self.variables,
                                   self.enable_local_save,
                                   self.temp_gcs_location
                                   ) as ds:


            fields['num_of_elements'] = math.prod([v for k,v in ds.sizes.items()])
            t = Timer(name=f'TTT {HOSTNAME}: Preparing: {pathlib.Path(uri).name}:{fields["num_of_elements"]}',
                      text='{name}: {:.2f} seconds', logger=logger.info)
            t.start()
            fields['attrs'] = ds.attrs
            fields['uri'] = uri
            fields.update({k: dataclasses.asdict(self)[k] for k in FileOutput._fields if k in dataclasses.asdict(self)})

            if self.variables and not self.infer_schema and not ds.attrs['is_normalized']:
                logger.info('Creating schema from input variables.')
                fields['schema'] = to_table_schema(
                    [('latitude', 'FLOAT64'), ('longitude', 'FLOAT64'), ('time', 'TIMESTAMP')] +
                    [(var, 'FLOAT64') for var in self.variables]
                )
            else:
                fields['schema'] = dataset_to_table_schema(ds)


            t.stop()

            yield FileOutput(**fields)



class ProcessFiles(beam.DoFn, beam.transforms.core.RestrictionProvider):
    """Splittable DoFn that will process FileOutput

    Yields:

    """

    def __init__(self):

        self.uris = []
        self.temp_gcs_location = None
        super().__init__()


    # NOTE(bahmandar): This will run if job is "stopped"
    def teardown(self):
        delete_local_temp_files(self.uris)
        # delete_remote_temp_files(self.uris, self.temp_gcs_location)
        super().teardown()

    def finish_bundle(self):
        delete_local_temp_files(self.uris)
        super().finish_bundle()

    def start_bundle(self):
        self.uris = []
        # NOTE(bahmandar): Might be better elsewhere? This fills in DATA_IMPORT_TIME_COLUMN
        self.current_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        super().start_bundle()

    def initial_restriction(self, element: FileOutput):
        return OffsetRange(0, element.num_of_elements)

    def create_tracker(self, restriction):
        return beam.io.restriction_trackers.OffsetRestrictionTracker(restriction)

    # NOTE(bahmandar): This dictates the immediate splitting. smaller number
    def split(self, element: FileOutput, restriction):
        i = restriction.start
        split_size = element.coordinate_chunk_size
        while i < restriction.stop - split_size:
            yield OffsetRange(i, i + split_size)
            i += split_size
        yield OffsetRange(i, restriction.stop)

    def restriction_size(self, element, restriction):
        return restriction.size()

    def process(self, element: FileOutput,
                tracker=beam.DoFn.RestrictionParam()) -> t.Iterable[t.Dict]:
        self.temp_gcs_location = element.temp_gcs_location
        # NOTE(bahmandar): Adding uris to cleanup later
        self.uris.append(element.uri)
        with open_dataset(element.uri,
                          element.xarray_open_dataset_kwargs,
                          element.disable_in_memory_copy,
                          element.disable_grib_schema_normalization,
                          element.tif_metadata_for_datetime,
                          element.area,
                          element.variables,
                          element.enable_local_save,
                          element.temp_gcs_location
                          ) as ds:

            current_position = tracker.current_restriction().start
            file_name = pathlib.Path(element.uri).name
            yield pvalue.TaggedOutput('uri', element.uri)

            while tracker.try_claim(current_position):
                end = tracker.current_restriction().stop

                t = Timer(name=f'TTT {HOSTNAME}: Processing: {file_name}, Start: {current_position}, End: {end}, Range: {end-current_position}', text='{name}: {:.2f} seconds', logger=logger.info)
                t.start()

                # NOTE(bahmandar): This will return a list of dictionarys based on the shape
                # of the dimensions. i.e. {time:0, longitude:0, latitude:0, depth:0},
                # {time:1, longitude:10, latitude:2, depth:0}, {time:1, longitude:2, latitude:2, depth:0}
                result = get_iterator_minimal(ds.sizes, current_position, end)


                # NOTE(bahmandar): Turn the result above to a list of tuples
                # [(longitude, 0),(longitude, 1),(depth, 0) ...]
                result_tuple_list = [(k, v) for f in result for k, v in f.items()]
                # NOTE(bahmandar): This will give me the max and min of each dimensions
                # as a dict {time:0, longitude:0, latitude:0, depth:0} and
                #{time:1, longitude:740, latitude:1440, depth:20}
                first = dict(sorted(result_tuple_list, reverse=True))
                last = dict(sorted(result_tuple_list, reverse=False))

                # NOTE(bahmandar): the goal of this below is to make a dict that
                # looks something like this {time:slice(0,1), longitude:slice(0,700), latitude:slice(0,1440), depth:slice(0,20)}
                output_dict = {}
                for key, value in first.items():
                    output_dict[key] = slice(first[key], last[key]+1, 1)
                # NOTE(bahmandar): loading this now to memory is much faster than
                # loading one by one in the for loop below
                sliced_ds_large = ds.isel(**output_dict, drop=True).compute()

                for idx, item in enumerate(result, start=1):
                    row = {}
                    # NOTE(bahmandar): Need to add an offset because we are using
                    # sliced_ds_large instead of the original dataset
                    item = {k: item.get(k, 0) - first.get(k, 0) for k in item.keys() | first.keys()}
                    sliced_ds = sliced_ds_large.isel(**item)

                    for key, value in dict(sliced_ds.variables, **sliced_ds.coords).items():
                        row[key] = to_json_serializable_type(ensure_us_time_resolution(value.data))

                    if not element.import_time:
                        import_time = self.current_time
                    else:
                        import_time = element.import_time

                    row[DATA_IMPORT_TIME_COLUMN] = import_time
                    row[DATA_URI_COLUMN] = element.uri

                    if 'latitude' in row and 'longitude' in row:
                        row[GEO_POINT_COLUMN] = fetch_geo_point(row['latitude'], row['longitude'])
                    elif 'lat' in row and 'lon' in row:
                        row[GEO_POINT_COLUMN] = fetch_geo_point(row['lat'], row['lon'])
                    yield pvalue.TaggedOutput('data', row)
                    # NOTE(bahmandar): without this statement dataflow would give
                    # warnings of process has gone for xxx seconds without a response
                    # this will also let dataflow know the progress for increasing workers
                    if (current_position + idx) >= end or not tracker.try_claim(current_position + idx):
                        break
                t.stop()
                current_position += end

