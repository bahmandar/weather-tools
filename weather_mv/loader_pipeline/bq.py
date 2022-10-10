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
from apache_beam.io.gcp.internal.clients import bigquery as bigquery_beam
from xarray.core.utils import ensure_us_time_resolution
import pathlib
from .sinks import ToDataSink
from .sinks import delete_remote_temp_file
from apache_beam.typehints import Iterable
from apache_beam.typehints import Tuple
from apache_beam.io import fileio
import math
from apache_beam.transforms import window
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.coders.coders import VarIntCoder
from oauth2client.client import GoogleCredentials

import time
from .util import (
    to_json_serializable_type,
    validate_region,
    _only_target_vars,
    get_coordinates,
    get_iterator_minimal,
    clean_label_value,
    get_user_email_from_credentials,
    str2bool,
    parse_area_list

)
from .transform_utils import (
    FanOut,
    WindowPerFile,
    ProcessFiles,
    PrepareFiles,
    DATA_URI_COLUMN
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

# NOTE(bahmandar): function is not needed anymore
def format_records(element) -> t.Dict:
    # output_dict = {"properties": {}}
    output_dict = {}
    for item in [i for sub in element for i in sub]:
        output_dict.update(item)
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
        # NOTE(bahmandar): need to get the num of elements for each file so the example_uri
        # is no longer needed
        # example_uri: URI to a weather data file, used to infer the BigQuery schema.
        output_table: The destination for where data should be written in BigQuery
        variables: Target variables (or coordinates) for the BigQuery schema. By default,
          all data variables will be imported as columns.
        area: Target area in "[N, W, S, E]"; by default, all available area is included.
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
        
        # NOTE(bahmandar): This will now dictate the immediate splitting of the
        # splittable dofn. How fast to parallelize across workers
        coordinate_chunk_size: How many coordinates (e.g. a cross-product of lat/lng/time
          xr.Dataset coordinate indexes) to group together into chunks. Used to tune
          how data is loaded into BigQuery in parallel.
          
        # NOTE(bahmandar): option to save locally if wanted
        enable_local_save: Will save files to each local VM. Default is False

    .. _these docs: https://beam.apache.org/documentation/io/built-in/google-bigquery/#setting-the-insertion-method
    """
    # example_uri: str
    temp_gcs_location: str
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
    coordinate_chunk_size: int = None
    table_schema: bigquery_beam.TableSchema = None
    project: str = None
    enable_local_save: bool = None


    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('-o', '--output_table', type=str, required=True,
                               help="Full name of destination BigQuery table (<project>.<dataset>.<table>). Table "
                                    "will be created if it doesn't exist.")
        subparser.add_argument('-v', '--variables', metavar='variables', type=str, nargs='+', default=None,
                               help='Target variables (or coordinates) for the BigQuery schema. Default: will import '
                                    'all data variables as columns.')
        subparser.add_argument('-a', '--area', metavar='area', type=parse_area_list, default=None,
                               help='Target area in "[N, W, S, E]" or "[latitude_max, longitude_min, latitude_min, longitude_max]"'
                                    'Default: Will include all available area')
        subparser.add_argument('--import_time', type=str, default=datetime.datetime.utcnow().isoformat(),
                               help=("When writing data to BigQuery, record that data import occurred at this "
                                     "time (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC."))
        subparser.add_argument('--infer_schema', type=str2bool, nargs='?', const=True, default=True,
                               help='Download one file in the URI pattern and infer a schema from that file. Default: '
                                    'on')
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default=None,
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--disable_in_memory_copy', type=str2bool, nargs='?', const=True, default=False,
                               help="To disable in-memory copying of dataset. Default: False")
        subparser.add_argument('--tif_metadata_for_datetime', type=str, default=None,
                               help='Metadata that contains tif file\'s timestamp. '
                                    'Applicable only for tif files.')
        subparser.add_argument('-s', '--skip_region_validation', type=str2bool, nargs='?', const=True, default=False,
                               help='Skip validation of regions for data migration. Default: off')
        subparser.add_argument('--coordinate_chunk_size', type=int, default=None,
                               help='The size of the chunk of coordinates used for extracting vector data into '
                                    'BigQuery. Used to tune parallel uploads.')
        subparser.add_argument('--disable_grib_schema_normalization', type=str2bool, nargs='?', const=True, default=False,
                               help="To disable grib's schema normalization. Default: off")
        subparser.add_argument('--enable_local_save', type=str2bool, nargs='?', const=True, default=None,
                               help="To enable saving files to each vm Default: off")
        subparser.add_argument('--public_ips', type=str2bool, nargs='?', const=True, default=False,
                               help="To enable the use of public ips Default: off")

    @classmethod
    def validate_arguments(cls, known_args: t.Any, pipeline_options: PipelineOptions) -> None:

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

    # NOTE(bahmandar): This will process all the schemas received from different
    # files
    def get_schema(self, sets):
        table_schema = []
        schema_fields = []
        elements = sets[1]
        attrs_list = []
        for item in elements:
            attrs_list.append(item.attrs)
            item_schema_names = [x.name for x in table_schema]
            for schema_field in item.schema:
                if schema_field.name not in item_schema_names:
                    schema_fields.append(schema_field)
                    table_schema.append(bigquery.SchemaField(name=schema_field.name,field_type=schema_field.type,mode=schema_field.mode, description=schema_field.description))
        if elements[0].dry_run:
            logger.debug('Created the BigQuery table with schema...')
            table = None
            output_schema = None

        else:
            # Create the table in BigQuery
            try:
                client = bigquery.Client()
                table = bigquery.Table(self.output_table.replace(":", "."), schema=table_schema)
                credentials = GoogleCredentials.get_application_default()
                table = client.create_table(table, exists_ok=True)
                unique_attrs = [k for j, k in enumerate(attrs_list) if k not in attrs_list[j + 1:]]
                unique_attrs_dict = {k: v for d in unique_attrs for k, v in d.items()}


                labels = {}
                unique_attrs_dict_serializable = {}
                for key, value in unique_attrs_dict.items():
                    # NOTE(bahmandar): 64 label limit for table. need to do subset
                    if key.lower() in ['product_name', 'title', 'license', 'source', 'history', 'conventions']:
                        labels[clean_label_value(key)] = clean_label_value(value)
                    unique_attrs_dict_serializable[key] = to_json_serializable_type(value)

                labels['system'] = 'weather-mv'
                user_email = get_user_email_from_credentials(credentials)
                if user_email:
                    labels['user'] = clean_label_value(user_email)
                labels['service_account_email'] = clean_label_value(client.get_service_account_email())



                table.labels = labels
                table = client.update_table(table, ["labels"])  # API request
                table.description = json.dumps(unique_attrs_dict_serializable, indent=2)
                table = client.update_table(table, ["description"])
            except Exception as e:
                logger.error(f'Unable to create table in BigQuery: {e}')
                raise
        # elements = list(map(lambda n: self.set_elment_table(n, table=table), elements))
        return schema_fields



    def get_table_input(self, element):
        logger.info(f'TTT: getting table info {self.output_table} from {element} ')
        return self.output_table


    # NOTE(bahmandar): This is no longer needed each file will be scanned for 
    # the schema because we already have to scan the file for the number of 
    # elements
    
    # def __post_init__(self):
    #     """Initializes Sink by creating a BigQuery table based on user input."""
        # with open_dataset(self.example_uri, self.xarray_open_dataset_kwargs, True,
        #                   self.disable_grib_schema_normalization, self.tif_metadata_for_datetime) as open_ds:
        #     # Define table from user input
        #     if self.variables and not self.infer_schema and not open_ds.attrs['is_normalized']:
        #         logger.info('Creating schema from input variables.')
        #         table_schema = to_table_schema(
        #             [('latitude', 'FLOAT64'), ('longitude', 'FLOAT64'), ('time', 'TIMESTAMP')] +
        #             [(var, 'FLOAT64') for var in self.variables]
        #         )
        #     else:
        #         logger.info('Inferring schema from data.')
        #         ds: xr.Dataset = _only_target_vars(open_ds, self.variables)
        #         table_schema = dataset_to_table_schema(ds)
        #
        # if self.dry_run:
        #     logger.debug('Created the BigQuery table with schema...')
        #     logger.debug(f'\n{pformat(table_schema)}')
        #     return
        #
        # # Create the table in BigQuery
        # try:
        #     table = bigquery.Table(self.output_table, schema=table_schema)
        #     self.table = bigquery.Client().create_table(table, exists_ok=True)
        # except Exception as e:
        #     logger.error(f'Unable to create table in BigQuery: {e}')
        #     raise

    def expand(self, paths):

        """Extract rows of variables from data paths into a BigQuery table."""

        prepared_files = (
            paths
            # NOTE(bahmandar): PrepareFile dataclass variables are being filled
            # with what is in the ToBigQuery class
            | 'Prepare Files' >> beam.ParDo(PrepareFiles(*(dataclasses.asdict(self).get(k.name) for k in dataclasses.fields(PrepareFiles))))
            # NOTE(bahmandar): shuffled in case of fusion. Might not be needed
            # | 'Shuffle' >> beam.Reshuffle()
            
            # NOTE(bahmandar): the three transforms below are not utilized anymore
            # this was used if each variable in the file was processed by itself
            # individually and then grouped together in rows based on the unique
            # coords (which was the key)
            # | 'Add Window Per File' >> WindowPerFile()

            # | 'Group by Key' >> beam.GroupByKey()
            # | 'Format Records' >> beam.Map(format_records)

        )


        # prepared_files = (
        #   paths
        #   | 'Add Constant Key To URIs' >> beam.Map(lambda v: (0, v))
        #   | 'Group URIs' >> beam.GroupByKey()
        #   | 'Prepare Group' >> beam.Map(self.get_schema)
        #   | 'FanOut' >>
        #   # NOTE(bahmandar): PrepareFile dataclass variables are being filled
        #   # with what is in the ToBigQuery class
        #   | 'Prepare Files' >> beam.ParDo(PrepareFiles(*(dataclasses.asdict(self).get(k.name) for k in dataclasses.fields(PrepareFiles))))
        #   # NOTE(bahmandar): shuffled in case of fusion. Might not be needed
        #   | 'Shuffle' >> beam.Reshuffle()
        #
        #   # NOTE(bahmandar): the three transforms below are not utilized anymore
        #   # this was used if each variable in the file was processed by itself
        #   # individually and then grouped together in rows based on the unique
        #   # coords (which was the key)
        #   | 'Add Window Per File' >> WindowPerFile()
        #
        #   # | 'Group by Key' >> beam.GroupByKey()
        #   # | 'Format Records' >> beam.Map(format_records)
        #
        # )



        # NOTE(bahmandar): a "schema" is created for each file and then turned into
        # a list
        schema = beam.pvalue.AsList(
            prepared_files
            | 'Add Key' >> beam.Map(lambda v: (0, v))
            | 'Group Schemas' >> beam.GroupByKey()
            | 'Create Schema' >> beam.Map(self.get_schema)

        )

        extracted_rows = (
            prepared_files | 'Process Files' >> beam.ParDo(ProcessFiles())
        )


        # NOTE(bahmandar): This was not done in teardown because other dofns
        # could still be using the gcs temp files
        (
          extracted_rows
          | 'Extract Uris' >> beam.Map(lambda x: x.get(DATA_URI_COLUMN))
          | 'Dedupe Uris' >> beam.Distinct()
          | 'Clean Remote Temp Files' >> beam.Map(lambda x: delete_remote_temp_file(x, self.temp_gcs_location))
        )

        if not self.dry_run:
            (
                    extracted_rows
                    | 'WriteToBigQuery' >> WriteToBigQuery(
                        project=self.project,
                        table=self.output_table,
                        schema=lambda table_info, side_input: bigquery_beam.TableSchema(fields=side_input[0]),
                        schema_side_inputs=(schema,),
                        method=WriteToBigQuery.Method.FILE_LOADS,
                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                        additional_bq_parameters={
                            'schemaUpdateOptions': [
                                'ALLOW_FIELD_ADDITION',
                                # 'ALLOW_FIELD_RELAXATION',
                            ]
                        })
            )
        else:
            pass
            # NOTE(bahmandar): needs testing
            (
                    extracted_rows
                    | 'Log Extracted Rows' >> beam.Map(logger.debug)
            )






