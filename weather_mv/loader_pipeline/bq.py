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
import argparse
import dataclasses
import datetime
import json
import logging
import os

# TYPING VS TYPEHINTS?
import typing as t
from pprint import pformat



import apache_beam as beam


from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from xarray.core.utils import ensure_us_time_resolution
import pathlib
from .sinks import ToDataSink, open_dataset, open_dataset_modified
from apache_beam.typehints import Iterable
from apache_beam.typehints import Tuple
from apache_beam.io import fileio
import math
from apache_beam.transforms import window
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.coders.coders import VarIntCoder
import time
from .util import (
    to_json_serializable_type,
    validate_region,
    _only_target_vars,
    get_coordinates,
    get_iterator_minimal
)
from .transform_utils import (
    FanOut,
    WindowPerFile,
    ProcessXArray,
    PrepareFiles
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()
# DATA_IMPORT_TIME_COLUMN = 'data_import_time'
# DATA_URI_COLUMN = 'data_uri'
# DATA_FIRST_STEP = 'data_first_step'
# GEO_POINT_COLUMN = 'geometry'
# GEO_TYPE = 'type'
# LATITUDE_RANGE = (-90, 90)





def merge_dicts(element):
    row_data = element[0]
    for item in element[1]:
        row_data.update(item)



def print_row(element):
    print('asssd')
    print(element)

    return element[1]

def format_records(element) -> t.Dict:
    # output_dict = {"properties": {}}
    output_dict = {}
    for item in [i for sub in element for i in sub]:
        output_dict.update(item)
        # if next(iter(item)) in ('geometry', 'type'):
        #     output_dict.update(item)
        # else:
        #     output_dict['properties'].update(item)

    return output_dict



@dataclasses.dataclass
class ToBigQuery(ToDataSink):
    """Load weather data into Google BigQuery.

    A sink that loads de-normalized weather data into BigQuery. First, this sink will
    create a BigQuery table from user input (either from `variables` or by inferring the
    schema). Next, it will convert the weather data into rows and then write each row to
    the BigQuery table.

    During a batch job, this transform will use the BigQueryWriter's file processing
    step, which requires that a `temp_location` is passed into the main CLI. This
    transform will perform streaming writes to BigQuery during a streaming Beam job. See
    `these docs`_ for more.

    Attributes:
        # example_uri: URI to a weather data file, used to infer the BigQuery schema.
        output_table: The destination for where data should be written in BigQuery
        variables: Target variables (or coordinates) for the BigQuery schema. By default,
          all data variables will be imported as columns.
        area: Target area in [N, W, S, E]; by default, all available area is included.
        import_time: The time when data was imported. This is used as a simple way to
          version data — variables can be distinguished based on import time. If None,
          the system will recompute the current time upon row extraction for each file.
        infer_schema: If true, this sink will attempt to read in an example data file
          read all its variables, and generate a BigQuery schema.
        xarray_open_dataset_kwargs: A dictionary of kwargs to pass to xr.open_dataset().
        disable_in_memory_copy: A flag to turn in-memory copy off; Default: in-memory copy enabled.
        tif_metadata_for_datetime: If the input is a .tif file, parse the tif metadata at
          this location for a timestamp.
        skip_region_validation: Turn off validation that checks if all Cloud resources
          are in the same region.
        disable_grib_schema_normalization: Turn off grib's schema normalization; Default: normalization enabled.
        coordinate_chunk_size: How many coordinates (e.g. a cross-product of lat/lng/time
          xr.Dataset coordinate indexes) to group together into chunks. Used to tune
          how data is loaded into BigQuery in parallel.

    .. _these docs: https://beam.apache.org/documentation/io/built-in/google-bigquery/#setting-the-insertion-method
    """
    # example_uri: str
    output_table: str
    variables: t.List[str]
    area: t.Tuple[int, int, int, int]
    import_time: t.Optional[datetime.datetime]
    infer_schema: bool
    xarray_open_dataset_kwargs: t.Dict
    disable_in_memory_copy: bool
    tif_metadata_for_datetime: t.Optional[str]
    skip_region_validation: bool
    disable_grib_schema_normalization: bool
    coordinate_chunk_size: int = 10_000
    disable_local_save: bool = False
    table_schema: t.List[bigquery.SchemaField] = None
    project: str = None

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('-o', '--output_table', type=str, required=True,
                               help="Full name of destination BigQuery table (<project>.<dataset>.<table>). Table "
                                    "will be created if it doesn't exist.")
        subparser.add_argument('-v', '--variables', metavar='variables', type=str, nargs='+', default=None,
                               help='Target variables (or coordinates) for the BigQuery schema. Default: will import '
                                    'all data variables as columns.')
        subparser.add_argument('-a', '--area', metavar='area', type=int, nargs='+', default=None,
                               help='Target area in [N, W, S, E]. Default: Will include all available area.')
        subparser.add_argument('--import_time', type=str, default=datetime.datetime.utcnow().isoformat(),
                               help=("When writing data to BigQuery, record that data import occurred at this "
                                     "time (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC."))
        subparser.add_argument('--infer_schema', action='store_true', default=False,
                               help='Download one file in the URI pattern and infer a schema from that file. Default: '
                                    'off')
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--disable_in_memory_copy', action='store_true', default=False,
                               help="To disable in-memory copying of dataset. Default: False")
        subparser.add_argument('--tif_metadata_for_datetime', type=str, default=None,
                               help='Metadata that contains tif file\'s timestamp. '
                                    'Applicable only for tif files.')
        subparser.add_argument('-s', '--skip-region-validation', action='store_true', default=False,
                               help='Skip validation of regions for data migration. Default: off')
        subparser.add_argument('--coordinate_chunk_size', type=int, default=500_000,
                               help='The size of the chunk of coordinates used for extracting vector data into '
                                    'BigQuery. Used to tune parallel uploads.')
        subparser.add_argument('--disable_grib_schema_normalization', action='store_true', default=False,
                               help="To disable grib's schema normalization. Default: off")
        subparser.add_argument('--disable_local_save', action='store_true', default=False,
                               help="To disable local save to vm worker Default: off")

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options_dict = pipeline_options.get_all_options()

        if known_args.area:
            assert len(known_args.area) == 4, 'Must specify exactly 4 lat/long values for area: N, W, S, E boundaries.'

        # Check that all arguments are supplied for COG input.
        _, uri_extension = os.path.splitext(known_args.uris)
        if uri_extension == '.tif' and not known_args.tif_metadata_for_datetime:
            raise RuntimeError("'--tif_metadata_for_datetime' is required for tif files.")
        elif uri_extension != '.tif' and known_args.tif_metadata_for_datetime:
            raise RuntimeError("'--tif_metadata_for_datetime' can be specified only for tif files.")

        # Check that Cloud resource regions are consistent.
        if not (known_args.dry_run or known_args.skip_region_validation):
            # Program execution will terminate on failure of region validation.
            logger.info('Validating regions for data migration. This might take a few seconds...')
            validate_region(known_args.output_table, temp_location=pipeline_options_dict.get('temp_location'),
                            region=pipeline_options_dict.get('region'))
            logger.info('Region validation completed successfully.')

    def set_elment_table(self, element, table):
        return element._replace(table=table)

    def get_schema(self, sets):
        table_schema = []
        elements = sets[1]
        for item in elements:
            item_schema_names = [x.name for x in table_schema]
            for schema_field in item.schema:
                if schema_field.name not in item_schema_names:
                    table_schema.append(schema_field)
        if elements[0].dry_run:
            logger.debug('Created the BigQuery table with schema...')
            table = None
        else:
            # Create the table in BigQuery
            try:
                # table_id = "your-project.your_dataset.your_table_name"
                table = bigquery.Table(self.output_table.replace(":","."), schema=table_schema)
                table = bigquery.Client().create_table(table, exists_ok=True)
            except Exception as e:
                logger.error(f'Unable to create table in BigQuery: {e}')
                raise
        elements = list(map(lambda n: self.set_elment_table(n, table=table), elements))


        return elements




    def __post_init__(self):
        """Initializes Sink by creating a BigQuery tzable based on user input."""
        logging.info('Post INIT')


    def expand(self, paths):

        """Extract rows of variables from data paths into a BigQuery table."""

        extracted_rows = (
            paths
            | 'Read Matches' >> fileio.ReadMatches()
            | 'Prepare Files' >> beam.ParDo(PrepareFiles(*(dataclasses.asdict(self).get(k.name) for k in dataclasses.fields(PrepareFiles))))
            | 'Get common items' >> beam.GroupByKey()
            | 'Create Schema' >> beam.Map(self.get_schema)
            | 'Fan out' >> beam.ParDo(FanOut())
            | 'Add Window Per File' >> WindowPerFile()
            | 'Process Files' >> beam.ParDo(ProcessXArray())
            | 'Group by Key' >> beam.GroupByKey()
            | 'Format Records' >> beam.Map(format_records)


        )
        #Issue with project??
        if not self.dry_run:
            (
                    extracted_rows
                    | 'WriteToBigQuery' >> WriteToBigQuery(
                        project=self.project,
                        table=self.output_table,
                        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                        create_disposition=BigQueryDisposition.CREATE_NEVER)
            )
        else:
            pass
            # (
            #         extracted_rows
            #         | 'Log Extracted Rows' >> beam.Map(logger.debug)
            # )






